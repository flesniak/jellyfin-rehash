#!/usr/bin/python3

import argparse
import concurrent.futures
import logging
import os
import sqlite3
import sys
import time
from hashlib import md5
from pathlib import Path
from uuid import UUID

def md5utf16(string):
    data = string.encode('utf-16le')
    return md5(data).digest()

# loosely adapted from https://github.com/jellyfin/jellyfin/blob/master/Emby.Server.Implementations/Library/LibraryManager.cs#L504 (GetNewItemIdInternal)
def hash(ProgramDataPath, key, typename, CaseSensitive=True):
    if key.startswith(ProgramDataPath):
        key = key[len(ProgramDataPath):].lstrip('/\\').replace('/','\\')
    if not CaseSensitive:
        key = key.lower()
    key = typename + key
    return UUID(bytes_le=md5utf16(key))

def migrate_paths(c, old_path, new_path):
    c.execute('update TypedBaseItems set Path=Replace(Path,?,?) where Path like ?', (old_path, new_path, old_path+'%'))
    c.execute('update TypedBaseItems set data=Replace(data,?,?) where data like ?', (old_path, new_path, '%'+old_path+'%'))
    c.execute('update TypedBaseItems set Images=Replace(Images,?,?) where Images like ?', (old_path, new_path, old_path+'%'))
    c.execute('update mediastreams set Path=Replace(Path,?,?) where Path like ?', (old_path, new_path, old_path+'%'))

def migrate_image_wrap(args):
    migrate_image(*args)

def migrate_image(RealMetadataPath, src, dst):
    if any((not x.startswith(RealMetadataPath) for x in [dst, src])):
        logging.warning(f'not in realmetadatapath: {src} -> {dst}')
        return 0
    dst_dir, _ = os.path.split(dst)
    try:
        logging.debug(f'mkdir {dst_dir}')
        os.makedirs(dst_dir, exist_ok=True)

        logging.debug(f'mv {src} {dst}')
        # stat = os.stat(src)
        os.rename(src, dst)
        # restore mtime to stop jellyfin from rewriting the db entry of this image
        # os.utime(dst, (stat.st_atime, stat.st_mtime))

        try:
            src_dir = os.path.split(src)[0]
            logging.debug(f'rmdir {src_dir}')
            os.rmdir(src_dir)
        except OSError:
            pass
    except OSError as e:
        logging.error(f'Failed to move {src} to {dst}: {e}')
    else:
        return 1
    return 0

def executemany(c, query, args):
    start = time.time()
    c.executemany(query, args)
    end = time.time()
    logging.info(f'Running {len(args)} queries "{query}", took {end - start:.2f}s')

def replace_file(path, patterns, replacement):
    with open(path, 'rt') as fin:
        text = fin.read()
    for pat in patterns:
        logging.debug(f'Replacing {pat} with {replacement} in {path}')
        text = text.replace(pat, replacement)
    with open(path, 'wt') as fout:
        fout.write(text)

def fix_collection_files(RealCollectionRoot, OldPaths, NewPath):
    for root, dirs, files in os.walk(RealCollectionRoot):
        for f in files:
            replace_file(f'{root}/{f}', OldPaths, NewPath)

def db_prepare(c):
    c.execute('PRAGMA synchronous = OFF')
    c.execute('PRAGMA journal_mode = MEMORY')
    c.execute('CREATE INDEX IF NOT EXISTS idx_SeasonIdTypedBaseItems on TypedBaseItems(SeasonId)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_SeriesIdTypedBaseItems on TypedBaseItems(SeriesId)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_TopParentIdTypedBaseItems on TypedBaseItems(TopParentId)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_PresentationUniqueKeyTypedBaseItems on TypedBaseItems(PresentationUniqueKey)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_SeriesPresentationUniqueKeyTypedBaseItems on TypedBaseItems(SeriesPresentationUniqueKey)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_UserDataKeyTypedBaseItems on TypedBaseItems(UserDataKey)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_ItemIdAncestorIds on AncestorIds(ItemId)')

def db_finalize(c):
    c.execute('DROP INDEX IF EXISTS idx_SeasonIdTypedBaseItems')
    c.execute('DROP INDEX IF EXISTS idx_SeriesIdTypedBaseItems')
    c.execute('DROP INDEX IF EXISTS idx_TopParentIdTypedBaseItems')
    c.execute('DROP INDEX IF EXISTS idx_PresentationUniqueKeyTypedBaseItems')
    c.execute('DROP INDEX IF EXISTS idx_SeriesPresentationUniqueKeyTypedBaseItems')
    c.execute('DROP INDEX IF EXISTS idx_UserDataKeyTypedBaseItems')
    c.execute('DROP INDEX IF EXISTS idx_ItemIdAncestorIds')

def rehash(args):
    RealRoot = Path(args.root) # can differ from ProgramDataPath when running in a container
    RealDataPath = RealRoot / args.data  # used to locate library.db
    RealMetadataPath = RealRoot / args.metadata # used to check and fix image paths
    RealCollectionRoot = RealRoot / args.collection # used to replace collection file definitions
    DBPath = RealDataPath / 'library.db'

    # sanity checks
    for name, path in {
        'root': RealRoot,
        'data': RealDataPath,
        'metadata': RealMetadataPath,
        'collection': RealCollectionRoot,
        'database': DBPath
    }.items():
        if path.exists():
            logging.info(f'Using jellyfin {name} path "{path}"')
        else:
            logging.error(f'{name} path "{path}" does not exist')
            sys.exit(1)
    logging.info(f'Using jellyfin internal data path {args.programdata}')
    logging.info(f'Using jellyfin media path {args.media_path}')

    conn = sqlite3.connect(DBPath)
    c = conn.cursor()
    db_prepare(c)

    if args.old_paths:
        logging.info(f'Fixing collection files in {RealCollectionRoot}')
        fix_collection_files(RealCollectionRoot, args.old_paths, args.media_path)

        for old_path in args.old_paths:
            logging.info(f'Migrating {old_path} to {args.media_path}...')
            migrate_paths(c, old_path, args.media_path)

    if args.kodi_sql:
        kodi_sql_file = open(args.kodi_sql, 'wt')
        kodi_sql_file.write('PRAGMA synchronous = NORMAL;  -- def: FULL\n')
        kodi_sql_file.write('PRAGMA journal_mode = WAL;    -- def: DELETE\n')
        kodi_sql_file.write('PRAGMA page_size = 4096;      -- def: 1024\n')
        kodi_sql_file.write('BEGIN TRANSACTION;\n')

    logging.info('Calculating new hashes and checking images')
    changed_types = list()
    guid_updates = list()
    image_updates = list()
    move_image_dirs = list() # from, to
    for guid_bytes, typename, name, path, images in c.execute('select guid,type,name,path,Images from TypedBaseItems where path like ?', (args.media_path+'%',)):
        if typename not in changed_types:
            changed_types += [typename]

        guid = UUID(bytes_le=guid_bytes)
        logging.debug(f'processing {name} ({guid}) {path}')

        new_guid = hash(args.programdata, path, typename, CaseSensitive=args.case_sensitive)
        if new_guid != guid:
            logging.debug(f'new hash {new_guid} for {path}')
            guid_updates += [{
                'new_raw': new_guid.bytes_le, # b'|\x17\x10i)\xdd\xcepX\xd9wm\xd0\xf0\xaa\x11'
                'new_hex': new_guid.hex, # '6910177cdd2970ce58d9776dd0f0aa11'
                'new_str': str(new_guid), # '6910177c-dd29-70ce-58d9-776dd0f0aa11'
                'old_raw': guid.bytes_le,
                'old_hex': guid.hex,
                'old_str': str(guid),
                'type': typename,
            }]
            if args.kodi_sql:
                kodi_sql_file.write(f'UPDATE files SET strFilename=replace(strFilename, "{guid.hex}", "{new_guid.hex}") WHERE strFilename like "%id={guid.hex}%";\n')
        else:
            logging.debug(f'hash unchanged for {path}')

        if args.move_images and RealMetadataPath is not None:
            if images is None:
                logging.debug(f'no images for {name}')
            else:
                imgs_split = images.split('|')
                imgs_result = list()
                needs_update = False

                for img in imgs_split:
                    old_img_path, attrs = img.split('*',1)
                    if old_img_path.startswith('%MetadataPath%'):
                        old_physical_path = old_img_path.replace('%MetadataPath%', str(RealMetadataPath))
                    elif old_img_path.startswith(args.programdata+'/metadata'):
                        old_physical_path = old_img_path.replace(args.programdata+'/metadata', str(RealMetadataPath))
                    else:
                        logging.debug('Skipping non-metadata image: {old_img_path}')
                        imgs_result += [img]
                        continue # do not touch images in media folders or web links
                    logging.debug(f'checking image {img}')
                    old_guid_filesystem = guid.hex
                    new_guid_filesystem = new_guid.hex
                    if guid != new_guid and old_guid_filesystem not in old_img_path:
                        logging.info(f'Keeping image not matching guid of {name}: {img}')
                        imgs_result += [img]
                        continue
                    old_subpath = f'/{old_guid_filesystem[:2]}/{old_guid_filesystem}'
                    new_subpath = f'/{new_guid_filesystem[:2]}/{new_guid_filesystem}'
                    new_img_path = old_img_path.replace(old_subpath, new_subpath)
                    new_physical_path = old_physical_path.replace(old_subpath, new_subpath)
                    if not os.path.exists(old_physical_path):
                        if not os.path.exists(new_physical_path):
                            logging.info(f'Removing nonexistent image of {name}: {old_img_path}')
                            continue
                        else:
                            needs_update = True # already moved, so fix the location
                    elif old_physical_path != new_physical_path:
                        move_image_dirs += [(old_physical_path, new_physical_path)]
                        needs_update = True
                        logging.debug(f'to -> {new_img_path}')
                    else:
                        pass # old and new paths match
                    imgs_result += [f'{new_img_path}*{attrs}']

                imgs_update = '|'.join(imgs_result) if len(imgs_result) > 0 else None
                if needs_update:
                    image_updates += [(imgs_update, new_guid.bytes_le)]

    if args.kodi_sql:
        kodi_sql_file.write('END TRANSACTION;\n')
        logging.info(f'Kodi SQL file written to {args.kodi_sql}')
    logging.info(f'updating {len(guid_updates)} hashes')

    # we have to do multiple iterations on TypedBaseItems as some keys are parent id mappings where both may need to be updated differently
    executemany(c, 'update TypedBaseItems set guid=:new_raw where guid=:old_raw', guid_updates)
    executemany(c, 'update TypedBaseItems set parentid=:new_raw where parentid=:old_raw', guid_updates)
    executemany(c, 'update TypedBaseItems set SeasonId=:new_raw where SeasonId=:old_raw', guid_updates)
    executemany(c, 'update TypedBaseItems set SeriesId=:new_raw where SeriesId=:old_raw', guid_updates)
    executemany(c, 'update TypedBaseItems set TopParentId=:new_hex where TopParentId=:old_hex', guid_updates)
    executemany(c, 'update TypedBaseItems set UserDataKey=:new_str where UserDataKey=:old_str', guid_updates)

    if args.old_paths:
        # data is special: For collection folders, it contains both its GUID as string and points to their physical path.
        executemany(c, 'update TypedBaseItems set data=Replace(data,:old_path,:new_path) where type="MediaBrowser.Controller.Entities.CollectionFolder"',
            [{'old_path': old_path, 'new_path': args.media_path} for old_path in args.old_paths])
        executemany(c, 'update TypedBaseItems set data=Replace(data,:old_str,:new_str) where type="MediaBrowser.Controller.Entities.CollectionFolder"',
            [x for x in guid_updates if x['type'] == 'MediaBrowser.Controller.Entities.Folder'])

    # PresentationUniqueKey is special. It may be:
    # - null
    # - 123456-XX-43cfe12fe7d9d8d21251e0964e0232e2 (external id, language and provider id?) [series and seasons only]
    # - ea840d8743744b04c606decfdaa5868a (GUID in hex form, season suffix) [series]
    # - ea840d8743744b04c606decfdaa5868a-001 (SeriesId in hex form = ParentId, season suffix) [series and seasons only]
    # - 9e5ee0206d5d6e5d19f6748aad5bac0d (GUID in hex form) [episodes only]
    # SeriesPresentationUniqueKey is related: If not null, it matches PresentationUniqueKey of its SeriesId base element (why not just do a join there?)
    # Thus, we need to do text-based replacements for PresentationUniqueKey columns to catch cases using GUID instead of external ids.
    executemany(c, 'update TypedBaseItems set PresentationUniqueKey=Replace(PresentationUniqueKey,:old_hex,:new_hex) '+
                   'where PresentationUniqueKey is not null and length(PresentationUniqueKey) < 37 and PresentationUniqueKey like :like_old_hex',
        [{**d, 'like_old_hex': f'{d["old_hex"]}%'} for d in guid_updates])
    executemany(c, 'update TypedBaseItems set SeriesPresentationUniqueKey=:new_hex where length(SeriesPresentationUniqueKey) = 32 and SeriesPresentationUniqueKey=:old_hex', guid_updates)

    executemany(c, 'update AncestorIds set AncestorId=:new_raw, AncestorIdText=:new_hex where AncestorId=:old_raw', guid_updates)
    executemany(c, 'update AncestorIds set ItemId=:new_raw where ItemId=:old_raw', guid_updates)

    executemany(c, 'update ItemValues set ItemId=:new_raw where ItemId=:old_raw', guid_updates)
    executemany(c, 'update People set ItemId=:new_raw where ItemId=:old_raw', guid_updates)
    executemany(c, 'update Chapters2 set ItemId=:new_raw where ItemId=:old_raw', guid_updates)
    executemany(c, 'update mediastreams set ItemId=:new_raw where ItemId=:old_raw', guid_updates)
    executemany(c, 'update mediaattachments set ItemId=:new_raw where ItemId=:old_raw', guid_updates)
    executemany(c, 'update UserDatas set key=:new_str where key=:old_str', guid_updates)
    # userdata table seems to be legacy
    # executemany(c, 'update userdata set key=:new_str where key=:old_str', guid_updates)

    if args.move_images:
        logging.info('fixing image references')
        executemany(c, 'update TypedBaseItems set Images=? where guid=?', image_updates)

        logging.info('moving images to new location')
        moved_images = 0
        for src, dst in move_image_dirs:
            moved_images += migrate_image(str(RealMetadataPath), src, dst)
        logging.info(f'moved {moved_images} image folders')

        # experimental process pool image moving - is this really faster?
        # migrate_image_args = [(str(RealMetadataPath), src, dst) for src, dst in move_image_dirs]
        # with concurrent.futures.ProcessPoolExecutor() as executor:
        #     moved_images = executor.map(migrate_image_wrap, migrate_image_args, chunksize=100)
        # logging.info(f'moved {sum(moved_images)} image folders')

    db_finalize(c)
    conn.commit()

    if not args.vacuum:
        logging.info('vacuuming database')
        c.execute('vacuum')

    conn.close()

    logging.debug('Changed entries of these types:')
    for typename in changed_types:
        logging.debug(f'  {typename}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Recalculate Jellyfin media GUIDs for changed media locations')
    parser.add_argument('-r', '--root', default='.', help='Physical root jellyfin data directory')
    parser.add_argument('-d', '--data', default='data', help='data subdirectory (default relative to root)')
    parser.add_argument('-M', '--metadata', default='metadata', help='metadata subdirectory (default relative to root)')
    parser.add_argument('-c', '--collection', default='root', help='collection root subdirectory (default relative to root)')
    parser.add_argument('-p', '--programdata', default='/config', help='data path from jellyfins perspective (different from root when running inside docker)')
    parser.add_argument('-k', '--kodi-sql', help='Write an SQL file to update MyVideosXXX.db when using Jellyfin for Kodi')
    parser.add_argument('-o', '--old-paths', action='append', help='Old media paths to convert to new media path')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print replacements')
    parser.add_argument('-q', '--quiet', action='store_true', help='Print warnings only')
    parser.add_argument('-I', '--no-move-images', dest='move_images', action='store_false', help='Neither change image paths in db nor move images to new location')
    parser.add_argument('-V', '--no-vacuum', dest='vacuum', action='store_false', help='Don't vacuum database after operation')
    parser.add_argument('-C', '--no-case-sensitive', dest='case_sensitive', action='store_false', help='Case insensitive hashes (current jellyfin versions default to case sensitive)')
    parser.add_argument('media_path', help='(new) media path to migrate to / rehash against')
    args = parser.parse_args()

    if args.old_paths is not None and args.media_path is None:
        parser.error('Migrating old paths requires specifying a media path')

    if args.verbose:
        level = logging.DEBUG
    elif args.quiet:
        level = logging.WARNING
    else:
        level = logging.INFO
    LOGGING_FORMAT = '%(asctime)-15s %(levelname)-8s %(message)s'
    logging.basicConfig(format=LOGGING_FORMAT, level=level)

    rehash(args)