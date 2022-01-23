import os


folders = ['exploitation_zone', 'featgen_zone', 'formatted_zone', 'trusted_zone']


def check_create_folders():
    for i in folders:
        if not os.path.exists(i):
            os.makedirs(i)
