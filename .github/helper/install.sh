#!/bin/bash
# Bootstraps a frappe-bench in CI with frappe (the telemetry client) + pulse on a
# single self-hosting test_site, ready for the Cypress e2e.
#
# FRAPPE_BRANCH selects the frappe branch to install (it carries the pulse
# telemetry client). Defaults to `develop`.
set -e

FRAPPE_BRANCH="${FRAPPE_BRANCH:-develop}"
FRAPPE_USER="${FRAPPE_USER:-frappe}"

cd ~ || exit

sudo apt update
sudo apt remove -y mysql-server mysql-client || true
sudo apt install -y libcups2-dev redis-server mariadb-client libmariadb-dev

pip install frappe-bench

git clone "https://github.com/${FRAPPE_USER}/frappe" --branch "${FRAPPE_BRANCH}" --depth 1
bench init --skip-assets --frappe-path ~/frappe --python "$(which python)" frappe-bench

mkdir ~/frappe-bench/sites/test_site
cp -r "${GITHUB_WORKSPACE}/.github/helper/site_config_mariadb.json" ~/frappe-bench/sites/test_site/site_config.json

mariadb --host 127.0.0.1 --port 3306 -u root -proot -e "SET GLOBAL character_set_server = 'utf8mb4'"
mariadb --host 127.0.0.1 --port 3306 -u root -proot -e "SET GLOBAL collation_server = 'utf8mb4_unicode_ci'"
mariadb --host 127.0.0.1 --port 3306 -u root -proot -e "CREATE USER 'test_frappe'@'localhost' IDENTIFIED BY 'test_frappe'"
mariadb --host 127.0.0.1 --port 3306 -u root -proot -e "CREATE DATABASE test_frappe"
mariadb --host 127.0.0.1 --port 3306 -u root -proot -e "GRANT ALL PRIVILEGES ON \`test_frappe\`.* TO 'test_frappe'@'localhost'"
mariadb --host 127.0.0.1 --port 3306 -u root -proot -e "FLUSH PRIVILEGES"

cd ~/frappe-bench || exit

# Keep web + redis; drop watcher/scheduler/socketio. We drain the pipeline
# manually in the test, so the scheduler must stay off to avoid races.
sed -i 's/watch:/# watch:/g' Procfile
sed -i 's/schedule:/# schedule:/g' Procfile
sed -i 's/socketio:/# socketio:/g' Procfile
sed -i 's/redis_socketio:/# redis_socketio:/g' Procfile

bench get-app pulse "${GITHUB_WORKSPACE}"
bench setup requirements --dev

bench start >> ~/frappe-bench/bench_start.log 2>&1 &
CI=Yes bench build --app frappe &
bench --site test_site reinstall --yes
