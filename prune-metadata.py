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
    if type(args) != 'generator':
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
    # c.execute('CREATE INDEX IF NOT EXISTS idx_SeasonIdTypedBaseItems on TypedBaseItems(SeasonId)')
    # c.execute('CREATE INDEX IF NOT EXISTS idx_SeriesIdTypedBaseItems on TypedBaseItems(SeriesId)')
    # c.execute('CREATE INDEX IF NOT EXISTS idx_TopParentIdTypedBaseItems on TypedBaseItems(TopParentId)')
    # c.execute('CREATE INDEX IF NOT EXISTS idx_PresentationUniqueKeyTypedBaseItems on TypedBaseItems(PresentationUniqueKey)')
    # c.execute('CREATE INDEX IF NOT EXISTS idx_SeriesPresentationUniqueKeyTypedBaseItems on TypedBaseItems(SeriesPresentationUniqueKey)')
    # c.execute('CREATE INDEX IF NOT EXISTS idx_UserDataKeyTypedBaseItems on TypedBaseItems(UserDataKey)')
    # c.execute('CREATE INDEX IF NOT EXISTS idx_ItemIdAncestorIds on AncestorIds(ItemId)')

def db_finalize(c):
    pass
    # c.execute('DROP INDEX IF EXISTS idx_SeasonIdTypedBaseItems')
    # c.execute('DROP INDEX IF EXISTS idx_SeriesIdTypedBaseItems')
    # c.execute('DROP INDEX IF EXISTS idx_TopParentIdTypedBaseItems')
    # c.execute('DROP INDEX IF EXISTS idx_PresentationUniqueKeyTypedBaseItems')
    # c.execute('DROP INDEX IF EXISTS idx_SeriesPresentationUniqueKeyTypedBaseItems')
    # c.execute('DROP INDEX IF EXISTS idx_UserDataKeyTypedBaseItems')
    # c.execute('DROP INDEX IF EXISTS idx_ItemIdAncestorIds')

def prune_metadata(args):
    RealRoot = Path(args.root) # can differ from ProgramDataPath when running in a container
    RealDataPath = RealRoot / args.data  # used to locate library.db
    RealMetadataPath = RealRoot / args.metadata # used to check and fix image paths
    # RealCollectionRoot = RealRoot / args.collection # used to replace collection file definitions
    DBPath = RealDataPath / 'library.db'

    if args.type == 'audio':
        prune_types = [
            'MediaBrowser.Controller.Entities.Audio.Audio',
            'MediaBrowser.Controller.Entities.Audio.MusicAlbum',
            'MediaBrowser.Controller.Entities.Audio.MusicArtist',
            'MediaBrowser.Controller.Entities.Audio.MusicGenre',
        ]
    else:
        logging.error(f'Metadata type {args.type} not known')
        sys.exit(1)

    # sanity checks
    for name, path in {
        'root': RealRoot,
        'data': RealDataPath,
        'metadata': RealMetadataPath,
        'database': DBPath
    }.items():
        if path.exists():
            logging.info(f'Using jellyfin {name} path "{path}"')
        else:
            logging.error(f'{name} path "{path}" does not exist')
            sys.exit(1)
    logging.info(f'Using jellyfin internal data path {args.programdata}')
    logging.info(f'Using jellyfin internal media path {args.mediadata}')

    conn = sqlite3.connect(DBPath)
    c = conn.cursor()
    db_prepare(c)

    unresolved_parents = list()
    roots = list()
    referenced_paths = list()
    changed_types = dict() # type -> count
    delete_parent_guids = list() # guid bytes_le to delete

    # get all parents of this media type
    for prune_type in prune_types:
        logging.info(f'Getting all metadata entries of type {prune_type}...')
        for parent_guid_bytes, path, images in c.execute('select ParentId,path,Images from TypedBaseItems where type=?', (prune_type,)):
            if not any((path.startswith(x) for x in ['%MetadataPath%', args.mediadata, args.programdata])):
                logging.warning(f'unknown path prefix in {path}')
            if not path.startswith(args.mediadata): # do not remove actual media
                referenced_paths += [path]
            if images is not None:
                img_paths = [img.split('*',1)[0] for img in images.split('|')]
                referenced_paths += img_paths
            if prune_type not in changed_types:
                changed_types[prune_type] = 1
            else:
                changed_types[prune_type] += 1
            if not parent_guid_bytes:
                continue
            unresolved_parents += [UUID(bytes_le=parent_guid_bytes)]

    logging.info(f'found {len(unresolved_parents)} parents')

    iteration = 0
    while len(unresolved_parents):
        iteration += 1
        logging.info(f'Resolving parents recursively, iteration {iteration}, {len(unresolved_parents)} parents remaining')
        new_unresolved_parents = []
        for guid in unresolved_parents:
            for guid_bytes, parent_guid_bytes, mediatype, path, images in c.execute('select guid,ParentId,type,path,Images from TypedBaseItems where guid=?', (guid.bytes_le,)):
                # stop if we reach an aggregate folder, i.e. the root of the directory tree
                if mediatype == 'MediaBrowser.Controller.Entities.AggregateFolder':
                    continue
                if not path.startswith(args.mediadata): # do not remove actual media
                    referenced_paths += [path]
                if images is not None:
                    img_paths = [img.split('*',1)[0] for img in images.split('|')]
                    referenced_paths += img_paths
                if not any((path.startswith(x) for x in ['%MetadataPath%', args.mediadata, args.programdata])):
                    logging.warning(f'unknown path prefix in {path}')
                if mediatype not in changed_types:
                    changed_types[mediatype] = 1
                else:
                    changed_types[mediatype] += 1
                if not guid.bytes_le in delete_parent_guids:
                    delete_parent_guids += [guid.bytes_le]
                new_unresolved_parents += [UUID(bytes_le=parent_guid_bytes)]
        unresolved_parents = list(set(new_unresolved_parents))

    executemany(c, 'delete from TypedBaseItems where type=?', list(((x,) for x in prune_types)))
    executemany(c, 'delete from TypedBaseItems where guid=?', list(((x,) for x in delete_parent_guids)))

    if args.delete_metadata_folders:
        delete_path_count = 0
        for path in referenced_paths:
            if path.startswith('%MetadataPath%/'):
                real_path = RealMetadataPath / path.removeprefix('%MetadataPath%/')
            elif path.startswith('/config/metadata/'):
                real_path = RealMetadataPath / path.removeprefix('/config/metadata/')
            elif path.startswith('root/metadata/'):
                # seems like root/* paths are purely virtual, so skip them
                continue
            # else:
            #     # real_path = RealDataPath
            #     print(f'real path {path}')
            try:
                if real_path.exists():
                    # logging.info(f'Leftover metadata path {real_path}')
                    delete_path_count += 1
                    real_path.unlink()
                    real_path.parent.rmdir()
                else:
                    logging.warning(f'Missing leftover metadata path {real_path}')
            except OSError as e:
                logging.warning(f'Failed to remove path: {e}')
        logging.info(f'Found {delete_path_count} paths to delete')

    # db_finalize(c)
    conn.commit()

    if not args.vacuum:
        logging.info('vacuuming database')
        c.execute('vacuum')

    conn.close()

    for typename, count in changed_types.items():
        logging.info(f'Removed {count} {typename} items')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Recalculate Jellyfin media GUIDs for changed media locations')
    parser.add_argument('-r', '--root', default='.', help='Physical root jellyfin data directory')
    parser.add_argument('-d', '--data', default='data', help='data subdirectory (default relative to root)')
    parser.add_argument('-M', '--metadata', default='metadata', help='metadata subdirectory (default relative to root)')
    parser.add_argument('-p', '--programdata', default='/config', help='data path from jellyfin perspective (different from root when running inside docker)')
    parser.add_argument('-m', '--mediadata', default='/media', help='media path from jellyfin perspective (different from root when running inside docker)')
    parser.add_argument('-t', '--type', default='audio', help='Type of metadata to prune (one of: audio, TODO)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print replacements')
    parser.add_argument('-q', '--quiet', action='store_true', help='Print warnings only')
    parser.add_argument('-D', '--no-delete-metadata', dest='delete_metadata_folders', action='store_false', help="Don't delete leftover metadata folders")
    parser.add_argument('-V', '--no-vacuum', dest='vacuum', action='store_false', help="Don't vacuum database after operation")
    args = parser.parse_args()

    if args.verbose:
        level = logging.DEBUG
    elif args.quiet:
        level = logging.WARNING
    else:
        level = logging.INFO
    LOGGING_FORMAT = '%(asctime)-15s %(levelname)-8s %(message)s'
    logging.basicConfig(format=LOGGING_FORMAT, level=level)

    prune_metadata(args)
