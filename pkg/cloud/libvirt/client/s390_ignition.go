package client

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	libvirt "github.com/libvirt/libvirt-go"
	"github.com/golang/glog"
	libvirtxml "github.com/libvirt/libvirt-go-xml"
	providerconfigv1 "github.com/openshift/cluster-api-provider-libvirt/pkg/apis/libvirtproviderconfig/v1beta1"
	"github.com/pkg/errors"
)

var execCommand = exec.Command

func setIgnitionForS390X(domainDef *libvirtxml.Domain, client *libvirtClient, ignition *providerconfigv1.Ignition, kubeClient kubernetes.Interface, machineNamespace, volumeName string) error {
	glog.Info("Creating ignition file for s390x")
	ignitionDef := newIgnitionDef()

	if ignition.UserDataSecret == "" {
		return fmt.Errorf("ignition.userDataSecret not set")
	}

	secret, err := kubeClient.CoreV1().Secrets(machineNamespace).Get(ignition.UserDataSecret, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("can not retrieve user data secret '%v/%v' when constructing cloud init volume: %v", machineNamespace, ignition.UserDataSecret, err)
	}
	userDataSecret, ok := secret.Data["userData"]
	if !ok {
		return fmt.Errorf("can not retrieve user data secret '%v/%v' when constructing cloud init volume: key 'userData' not found in the secret", machineNamespace, ignition.UserDataSecret)
	}

	ignitionDef.Name = volumeName
	ignitionDef.PoolName = client.poolName
	ignitionDef.Content = string(userDataSecret)

	glog.Infof("Ignition: %+v", ignitionDef)

	ignitionVolumeName, err := ignitionDef.createAndUploadIso(client)
	if err != nil {
		return fmt.Errorf("Error create and upload iso file: %s", err)
	}

	glog.Infof("[DEBUG] newDiskForConfigDrive for coreos_ignition on s390x ")
	disk, err := newDiskForConfigDrive(client.connection, ignitionVolumeName)
	if err != nil {
		return err
	}

	domainDef.Devices.Disks = append(domainDef.Devices.Disks, disk)

	return nil
}


func (ign *defIgnition) createAndUploadIso(client *libvirtClient) (string, error) {
	ignFile, err := ign.createFile()
	if err != nil {
		return "", err
	}
	defer func() {
		// Remove the tmp ignition file
		if err = os.Remove(ignFile); err != nil {
			glog.Infof("Error while removing tmp Ignition file: %s", err)
		}
	}()

	isoVolumeFile, err := createIgnitionISO(ign.Name, ignFile)
	if err != nil {
		return "", fmt.Errorf("Error generate iso file: %s", err)
	}

	img, err := newImage(isoVolumeFile)
	if err != nil {
		return "", err
	}

	size, err := img.size()
	if err != nil {
		return "", err
	}

	volumeDef := newDefVolume(ign.Name)
	volumeDef.Capacity.Unit = "B"
	volumeDef.Capacity.Value = size
	volumeDef.Target.Format.Type = "raw"

	return uploadVolume(ign.PoolName, client, volumeDef, img)
}

func newDiskForConfigDrive(virConn *libvirt.Connect, volumeKey string) (libvirtxml.DomainDisk, error) {
	disk := libvirtxml.DomainDisk{
		Device: "cdrom",
		Target: &libvirtxml.DomainDiskTarget{
			// s390 platform doesn't support IDE controller, it shoule be virtio controller
			Dev: "vdb",
			Bus: "scsi",
		},
		Driver: &libvirtxml.DomainDiskDriver{
			Name: "qemu",
			Type: "raw",
		},
	}
	diskVolume, err := virConn.LookupStorageVolByKey(volumeKey)
	if err != nil {
		return disk, fmt.Errorf("Can't retrieve volume %s: %v", volumeKey, err)
	}
	diskVolumeFile, err := diskVolume.GetPath()
	if err != nil {
		return disk, fmt.Errorf("Error retrieving volume file: %s", err)
	}

	disk.Source = &libvirtxml.DomainDiskSource{
		File: &libvirtxml.DomainDiskSourceFile{
			File: diskVolumeFile,
		},
	}

	return disk, nil
}


// createIgnitionISO create config drive iso with ignition-config file
func createIgnitionISO(ignName string, ignPath string) (string, error) {
	glog.Infof("DEBUG: ignName %s, ignPath s%", ignName, ignPath)
	//mkdir -p /tmp/new-drive/openstack/latest
	err := os.MkdirAll("/tmp/new-drive/openstack/latest", 0755)
	if err != nil {
		return "", fmt.Errorf("Error mkdir for /tmp/new-drive/openstack/latest : %s", err)
	}
	//get the ignition contentt
	userData, err := os.Open(ignPath)
	glog.Infof("DEBUG: ignition content %s", userData)
	if err != nil {
		return "", fmt.Errorf("Error get the ignition content : %s", err)
	}
	defer userData.Close()
	//cp user_data /tmp/new-drive/openstack/latest/user_data
	newDestinationPath, err := os.Create("/tmp/new-drive/openstack/latest/user_data")
	glog.Infof("DEBUG: newDestinationPath path %s", newDestinationPath)
	if err != nil {
		return "", fmt.Errorf("Error create the file for ignition : %s", err)
	}
	if _, err := io.Copy(newDestinationPath, userData); err != nil {
		return "", fmt.Errorf("Error copy the ignitio content to newDestinationPath : %s", err)
	}
	//genisoimage -o disk.config -ldots -allow-lowercase -allow-multidot -l -quiet -J -r -V config-2 ./
	glog.Infof("DEBUG: isoDestination path %s", ignPath)
	cmd := exec.Command(
		"genisoimage",
		"-o",
		ignPath,
		"-ldots",
		"-allow-lowercase",
		"-allow-multidot",
		"-l",
		"-quiet",
		"-J",
		"-r",
		"-V",
		"config-2",
		"/tmp/new-drive/")
	glog.Infof("About to execute cmd: %+v", cmd)

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("Error while starting the creation of ignition's ISO image: %s", err)
	}
	glog.Infof("ISO created at %s", ignPath)

	if err := os.RemoveAll("/tmp/new-drive/openstack"); err != nil {
		return "", fmt.Errorf("Error remove the file /tmp/new-drive/openstack: %s", err)
	}
	glog.Infof("Config drive image for %s created", ignName)
	return ignPath, nil
}

func injectIgnitionByGuestfish(domainDef *libvirtxml.Domain, ignitionFile string) error {
	glog.Info("Injecting ignition configuration using guestfish")

	runAsRoot := true

	/*
	 * Add the image into guestfish, execute the following command,
	 *     guestfish --listen -a ${volumeFilePath}
	 *
	 * output example:
	 *     GUESTFISH_PID=4513; export GUESTFISH_PID
	 */
	args := []string{"--listen", "-a", domainDef.Devices.Disks[0].Source.File.File}
	output, err := startCmd(runAsRoot, nil, args...)
	if err != nil {
		return err
	}

	strArray := strings.Split(output, ";")
	if len(strArray) != 2 {
		return fmt.Errorf("invalid output when starting guestfish: %s", output)
	}
	strArray1 := strings.Split(strArray[0], "=")
	if len(strArray1) != 2 {
		return fmt.Errorf("failed to get the guestfish PID from %s", output)
	}
	env := []string{strArray[0]}

	/*
	 * Launch guestfish, execute the following command,
	 *     guestfish --remote -- run
	 */
	args = []string{"--remote", "--", "run"}
	_, err = execCmd(runAsRoot, env, args...)
	if err != nil {
		return err
	}

	/*
	 * Get the boot filesystem, execute the following command,
	 *     findfs-label boot
	 *
	 * output example:
	 *     /dev/sda1
	 */
	args = []string{"--remote", "--", "findfs-label", "boot"}
	output, err = execCmd(runAsRoot, env, args...)
	if err != nil {
		return err
	}

	bootDisk := strings.TrimSpace(output)
	if len(bootDisk) == 0 {
		return fmt.Errorf("failed to get the boot filesystem")
	}

	/*
	 * Mount the boot filesystem, execute the following command,
	 *     guestfish --remote -- mount ${boot_filesystem} /
	 */
	args = []string{"--remote", "--", "mount", bootDisk, "/"}
	_, err = execCmd(runAsRoot, env, args...)
	if err != nil {
		return err
	}

	/*
	 * Upload the ignition file, execute the following command,
	 *     guestfish --remote -- upload ${ignition_filepath} /ignition/config.ign
	 *
	 * The target path is hard coded as "/ignition/config.ign" for now
	 */
	args = []string{"--remote", "--", "upload", ignitionFile, "/ignition/config.ign"}
	_, err = execCmd(runAsRoot, env, args...)
	if err != nil {
		return err
	}

	/*
	 * Umount all filesystems, execute the following command,
	 *     guestfish --remote -- umount-all
	 */
	args = []string{"--remote", "--", "umount-all"}
	_, err = execCmd(runAsRoot, env, args...)
	if err != nil {
		return err
	}

	/*
	 * Exit guestfish, execute the following command,
	 *     guestfish --remote -- exit
	 */
	args = []string{"--remote", "--", "exit"}
	_, err = execCmd(runAsRoot, env, args...)
	if err != nil {
		return err
	}

	return nil
}

func execCmd(runAsRoot bool, env []string, args ...string) (string, error) {
	cmd := genCmd(runAsRoot, env, args...)
	glog.Infof("Running: %v", cmd.Args)

	cmdOut, err := cmd.CombinedOutput()
	glog.Infof("Ran: %v Output: %v", cmd.Args, string(cmdOut))
	if err != nil {
		err = errors.Wrapf(err, "error running command '%v'", strings.Join(cmd.Args, " "))
	}
	return string(cmdOut), err
}

// startCmd starts the command, and doesn't wait for it to complete
func startCmd(runAsRoot bool, env []string, args ...string) (string, error) {
	cmd := genCmd(runAsRoot, env, args...)
	glog.Infof("Starting: %v", cmd.Args)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", errors.Wrapf(err, "error getting stdout pipe for command '%v'", strings.Join(cmd.Args, " "))
	}
	err = cmd.Start()
	glog.Infof("Started: %v", cmd.Args)
	if err != nil {
		return "", errors.Wrapf(err, "error starting command '%v'", strings.Join(cmd.Args, " "))
	}

	outMsg, err := readOutput(stdout)
	glog.Infof("Output message: %s", outMsg)

	return outMsg, err
}

func genCmd(runAsRoot bool, env []string, args ...string) *exec.Cmd {
	executable := "guestfish"
	newArgs := []string{}
	if runAsRoot {
		newArgs = append(newArgs, []string{"--preserve-env", executable}...)
		newArgs = append(newArgs, args...)
		executable = "sudo"
	} else {
		newArgs = args
	}
	cmd := execCommand(executable, newArgs...)
	if env != nil && len(env) > 0 {
		cmd.Env = append(cmd.Env, env...)
	}
	return cmd
}

func readOutput(stream io.ReadCloser) (string, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(stream)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}
