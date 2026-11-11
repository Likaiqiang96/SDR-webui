# 小工具

## usbreset

```bash
gcc -O2 -o usbreset usbreset.c
lsusb | grep 1df7          # Bus 001 Device 047: ID 1df7:2500 SDRplay RSP1
sudo ./usbreset /dev/bus/usb/001/047
```

SDRplay API 服务崩溃循环、或反复被 kill -9 之后设备会卡住（表现为
`no available RSP devices found` / `sdrplay_api_ServiceNotResponding`）。
先 `sudo systemctl restart sdrplay`，仍不行就用这个复位设备。
