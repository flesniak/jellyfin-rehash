# Jellyfin Re-Hasher

This script allows to change the path of media files in a Jellyfin database (tested with 10.6.4).
It is necessary re-calculate all GUID during this process, as the GUID is not random but calculated from a hash of the path and media type.
Additionally, metadata folders (images) are also migrated to match the new GUID, although this is not strictly necessary.

Example usage: Assuming your jellyfin config directory resides unter /docker/data/jellyfin and you want to change your metadata path from /media/old/location to /media/new_location, use:
```
./rehash.py -r /docker/data/jellyfin -o /media/old/location /media/new_location
```

**Make a backup of your config directory - this is highly experimental!**

## Updating Jellyfin for Kodi

When using the Jellyfin for Kodi addon, the play urls in its database need to be updated as well. The `-k` option creates an sql file that can be applied to your Kodi database:

```
sqlite3 ~/.kodi/userdata/Database/MyVideos116.db <kodi.sql
```
