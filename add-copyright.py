def add_copyright():

    import os

    # Create an array of paths mirroring structure of current directory
    # while ignoring this script and problematic file types
    paths = []
    for (root, dirs, files) in os.walk('.', topdown=True):
        for f in files:
            if f == 'add-copyright.py':
                continue
            elif '.data' in f:
                continue
            elif '.png' in f:
                continue
            elif '.jar' in f:
                continue
            elif '.' not in f:
                continue
            elif '.pack' in f:
                continue
            elif '.idx' in f:
                continue
            else:
                path = os.path.join(root, f)
                paths.append(path)
    print(paths)

    # Iterate through file paths, adding a copyright line to each file
    for p in paths:
        if '.circleci' in p:
            continue
        elif '.github' in p:
            continue 
        elif '.gitignore' in p:
            continue
        else:        
            if p[-4:] == 'java':
                with open(p, 'r') as t:
                    contents = t.readlines()
                    contents.insert(0, '/* Copyright 2018-2022 contributors to the OpenLineage project */\n\n')
                with open(p, 'w') as t:
                    contents = ''.join(contents)
                    t.write(contents)
            elif p[-2:] == 'py' or p[-2:] == 'sh':
                with open(p, 'r') as t:
                    contents = t.readlines()
                    contents.insert(0, '# Copyright 2018-2022 contributors to the OpenLineage project\n\n')
                with open(p, 'w') as t:
                    contents = ''.join(contents)
                    t.write(contents)
            elif p[-2:] == 'md':
                with open(p, 'r') as t:
                    contents = t.readlines()
                    contents.insert(0, '<!-- Copyright 2018-2022 contributors to the OpenLineage project -->\n\n')
                with open(p, 'w') as t:
                    contents = ''.join(contents)
                    t.write(contents)
            elif p[-3:] == 'txt':
                with open(p, 'r') as t:
                    contents = t.readlines()
                    contents.insert(0, 'Copyright 2018-2022 contributors to the OpenLineage project\n\n')
                with open(p, 'w') as t:
                    contents = ''.join(contents)
                    t.write(contents)
            elif p[-2:] == 'rs':
                with open(p, 'r') as t:
                    contents = t.readlines()
                    contents.insert(0, '// Copyright 2018-2022 contributors to the OpenLineage project\n\n')
                with open(p, 'w') as t:
                    contents = ''.join(contents)
                    t.write(contents)
            elif p[-2:] == 'sh':
                with open(p, 'r') as t:
                    contents = t.readlines()
                    contents.insert(0, '# Copyright 2018-2022 contributors to the OpenLineage project\n\n')
                with open(p, 'w') as t:
                    contents = ''.join(contents)
                    t.write(contents)

add_copyright()