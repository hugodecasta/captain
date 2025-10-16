[Unit]
Description=Sailor Worker Service
After=network.target

[Service]
Type=simple
WorkingDirectory=__ROOT__
ExecStart=__PYTHON__ __ROOT__/sailor.py --serve __PORT__
Restart=always
RestartSec=2
User=__USER__
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
