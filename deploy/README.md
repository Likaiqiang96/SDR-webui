# 部署

```bash
sudo cp sdr-webdash.service /etc/systemd/system/
sudo sed -i "s|__INSTALL_DIR__|$PWD/..|g; s|__USER__|$USER|g" /etc/systemd/system/sdr-webdash.service
sudo systemctl daemon-reload
sudo systemctl enable --now sdr-webdash
```

- `sdr-webdash.service`：C++ 后端（默认，推荐）
- `sdr-webdash-python.service`：Python 后端（备用/调试）

两个服务都依赖 `sdrplay.service`（SDRplay API），端口默认 8095。
