# Install on ubuntu
> From https://www.postgresql.org/download/linux/ubuntu/

- Use the apt repository
    1. Create the file `/etc/apt/sources.list.d/pgdg.list` and add a line for the repository
        `deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main`
    2. Import the repository signing key, and update the package lists
        ```sh
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
        sudo apt-get update
        ```
- Included in distribution  
    Ubuntu includes PostgreSQL by default. To install PostgreSQL on Ubuntu, use the apt (or other apt-driving) command:
    `$ sudo apt install postgresql-11`


