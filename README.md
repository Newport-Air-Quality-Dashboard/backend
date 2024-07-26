# Readme

## Deployment

### Backend

To begin, you clone the repo:

```bash
git clone https://www.github.com/Renewport-Dashboard/backend/
```

Afterwards copy the file `config.R.example` to `config.R`:

```bash
cp config.R.example config.R
```

Insert API keys for an AirNow (EPA) account and PurpleAir account where specified
and change any options that you'd like. Also specify the location of the database
you want to write to. I think that the frontend expects the database to be in
the same directory as it (e.g. `/srv/shiny-server/<app-dir>/db.sqlite`)

Then execute the `backend.R` script. I would recommend: 

- opening a terminal and running `tmux` 
- then navigating to the directory with `cd`
- making the file executable if it isn't (`chmod +x backend.R`)
- running it (`./backend.R`)
- then detaching from the tmux session (pressing `Ctrl+b` then `d`)

The benefits to doing this is the program is running in the background and will
continue executing as long as the machine is still on. To get back to the running
program type `tmux a` into a terminal.


## Frontend

Either install Ubuntu 24.04 or select it in a VPS setting. Install shiny-server:

```bash
sudo apt update && sudo apt upgrade && sudo apt install shiny-server
```

Also install dependencies required by some libraries:

```bash
sudo apt install -y libfontconfig1-dev
sudo apt install -y libudunits2-dev libgdal-dev libgeos-dev libproj-dev
```

then make sure shiny-server is set to auto-start:

```bash
sudo systemctl enable --now shiny-server.service
```

Ubuntu has a firewall, so you have to allow connections through it for
shiny-server:

```bash
sudo ufw allow 3838
```

then i would restart:

```bash
reboot
```

after that clone the shiny app repo into the directory shiny-server

```bash
cd /srv/shiny-server/
git clone https://www.github.com/ReNewport-Dashboard/RShinyApp/
```

then make sure that the shiny-server has write access:

```bash
chmod -R o+rw /srv/shiny-server
```

The app should be available on the device in a web browser at:

```bash
127.0.0.1:3838
```

Or on the local network at an ip address findable by the command:

```bash
ip addr
```

then go to:

```
<ip address>:3838
```

in a browser.

