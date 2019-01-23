# OPUS-API

## How to deploy

Clone this git:

`git clone https://github.com/miau1/OPUS-API.git`

Create a virtual environment and install required packages:

```
python3 -m venv opusapienv
source opusapienv/bin/activate
pip3 install flask sqlalchemy
deactivate
```
Move `opusdata.db` database to `/var/www/` and update permissions, so that user `www-data` can read it:

```
sudo mv OPUS-API/opusdata.db /var/www/
sudo chmod 774 /var/www/opusdata.db
```
Update your apache configuration file (located at `/etc/apache2/sites-available/000-default.conf` by default, remember to update server name and paths):

```
## /etc/apache2/sites-available/000-default.conf

<VirtualHost *:80>
  ServerName vm0024.kaj.pouta.csc.fi
  WSGIDaemonProcess opusapi python-path=/home/cloud-user/OPUS-API python-home=/home/cloud-user/opusapienv
  WSGIScriptalias / /home/cloud-user/OPUS-API/opusapi.wsgi

  <Directory /home/cloud-user/OPUS-API/>
    WSGIProcessGroup opusapi
    WSGIApplicationGroup %{GLOBAL}
    WSGIScriptReloading On

    Require all granted
  </Directory>

  ErrorLog ${APACHE_LOG_DIR}/error.log
  CustomLog ${APACHE_LOG_DIR}/access.log combined

</VirtualHost>

# vim: syntax=apache ts=4 sw=4 sts=4 sr noet
```

Restart apache2 server:

`sudo systemctl restart apache2`

And now you are able use the API, for example:

`http://vm0024.kaj.pouta.csc.fi/opusapi/?corpus=OpenSubtitles&source=en&target=fi&preprocessing=xml&version=latest`
