# Simple Sync Daemon

a simple file syncing utility to sync specific kinds of files and directories to my server, the reason im writing something from scratch instead of using rsync or rclone is because my server has a very specific set of rules for file sorting and distribution that would make maintaining those command flags tedious

## Usage

python3 main.py <dir1> <dir2> [dir3 ...]

Example:
python3 main.py ~/Documents ~/Backup/Documents

## Behavior

- Syncs directories from host to server ONLY
- Runs continuously in the background
- Detects file changes and mirrors them, excluding deletions
- No conflict detection, if you modify something on the host it WILL overwrite it on the server
- Does not delete files
- does not preserve permissions

## Requirements

- Python 3.10+
- Host and server need to be the same OS, if this is not true I dont know what kind of behavior emerges because I HAVE NOT TESTED THAT
- sftp running on server
- environment variables:
    - SYNCUSR: ssh username
    - SYNCPWD: ssh password
    - REMOTE: ip address of your server
    - PORT: port used for sftp
- valid config file (template provided), if this isnt provided the program will failif

## Note

If using the template as is the program will create any directories it cant find assuming your user has the required permissions

## Warning

This is not meant to be a general use tool, it is meant to solve my specific problem and as such I have not tested any edge cases or alternate use cases. I will not be assuming responsibility for data loss or any other kinds of damage that may arise from the use of this software

# LICENSE

I dont really see the utility for anyone other than me since this is way worse than any other project out there but its MIT licensed so do what you want with it lol, see [LICENSE](LICENSE.md) for more
