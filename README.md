# Captain/Sailor

A minimal resource scheduler with a Captain (controller) and Sailors (workers).

## Installation

### Common installation

Clone the repository
```bash
git clone https://github.com/hugodecasta/captain.git
cd captain
```

Install the system
```bash
sudo ./install.sh
```

Install the registry.\
The registry is a database accessible to all your system. It should be stored in a common area. Once setup, you will need to declare its location to all your crew (captain, lieutenant and sailors).
```bash
sudo captain --install-db <path to db>
```

### Lieutenant

The lieutenant will take care of the day to day chores assignement and archiving. It needs to know where the registry be able to run forever.

[`install and declare db`](#common-installation)

Launch the lieutenant
```bash
sudo lieutenant
```

Or make it run on its own
```bash
sudo lieutenant --create-service
sudo service lieutenant status
```

### Sailor

In order to add a sailor in the crew, one first need to declare it to the captain using the pre-register command

Declare the registry
```bash
sudo captain --prereg -n <name> -s <comma separated services>
```

Once the sailor as been preregistered, you can setup the sailor on its own machine.

[`install and declare db`](#common-installation)

Setup the local sailor data
```bash
sudo sailor --setup -n <name> -g <gpu count>
```

Launch your sailor
```bash
sudo sailor --run
```

Or make it work on its own
```bash
sudo sailor --create-service
sudo service sailor status
```

### Updates

In order to pull all updates, use the update command
```bash
cd captain
./update.sh
```

Don't forget to restart all service related to the boat(`sailor`, `lieutenant`)

## Usage

### User

```bash
captain --crew
```

```bash
captain --chore [-slr <sailor name>] [-srv <service name>] -wd <working dir> -sc <sh script relative to wd> --cpus <cpu count requested> --gpus <gpu count requested> --out <out file relative to wd>
```

```bash
captain --consult [--small]
```

```bash
captain --cancel -cid <chore ID>
```