// 复位 USB 设备（SDRplay API 卡死 / 设备 wedged 时用）
// 用法: sudo ./usbreset /dev/bus/usb/001/047
//   查设备号: lsusb | grep 1df7
#include <fcntl.h>
#include <linux/usbdevice_fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <unistd.h>

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s /dev/bus/usb/BBB/DDD\n", argv[0]);
        return 2;
    }
    int fd = open(argv[1], O_RDWR);
    if (fd < 0) { perror("open"); return 1; }
    int r = ioctl(fd, USBDEVFS_RESET, 0);
    if (r < 0) perror("ioctl(USBDEVFS_RESET)");
    else printf("reset ok: %s\n", argv[1]);
    close(fd);
    return r < 0 ? 1 : 0;
}
