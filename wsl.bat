$wsl_ip = (wsl -e sh -lc "hostname -I | awk '{print $1}'").Trim()

netsh interface portproxy delete v4tov4 listenaddress=192.168.186.87 listenport=4561

netsh interface portproxy add v4tov4 listenaddress=192.168.186.87 listenport=4561 connectaddress=$wsl_ip connectport=4561

netsh advfirewall firewall add rule name="WSL2_in_4561" dir=in action=allow protocol=TCP localip=192.168.186.87 localport=4561