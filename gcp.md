# Starting by watching [this video](https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=18)

## Create an SSH key

## Install Docker on Ubuntu

### [Docker without SUDO](https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md)

I can check by running a couple of commands

```bash
docker run -it hello-world
```

```bash
docker run -it ubuntu bash
```

### Install Docker Compose from [here](https://github.com/docker/compose/releases)

```bash
mkdir ~/bin && cd bin/
```

```bash
wget https://github.com/docker/compose/releases/download/v2.24.2/docker-compose-linux-x86_64 -O docker-compose
```

Do a chmod to make it executable

```bash
chmod +x docker-compose
```

Sanity check *cd into the zoomcamp directory to run the docker compose yaml file as a check*
