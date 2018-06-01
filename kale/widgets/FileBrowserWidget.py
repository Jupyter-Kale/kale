import os

import ipywidgets as ipw

class FileBrowserWidget(ipw.Select):
    def __init__(self):
        super().__init__()

        self.cwd = os.getcwd()
        self.options = self.ls()

        self.layout.height = u'300px'

        self.observe(self.perform_cd, names='index')

    def ls(self):
        full_ls = os.listdir(self.cwd)

        dirs = []
        files = []
        for file in full_ls:
            abspath = self.abspath(file)

            if os.path.isdir(abspath):
                dirs.append(file)
            else:
                files.append(file)

        # Format
        dirs = [self.format_dir(path) for path in self.sortfiles(dirs)]
        files = [self.format_file(path) for path in self.sortfiles(files)]

        return tuple([('.', '.'), ('..', '..')] + dirs + files)

    def format_dir(self, path):
        return (self.dirify(path), self.abspath(path))

    def format_file(self, path):
        return (path, self.abspath(path))

    def sortfiles(self, filelist):
        "Sort files alphabetically, placing hidden files after others."

        hidden = []
        nonhidden = []

        for file in filelist:
            if file[0] == '.':
                hidden.append(file)
            else:
                nonhidden.append(file)

        return sorted(nonhidden) + sorted(hidden)

    def dirify(self, path):
        "How to represent directory names"
        return '** {} **'.format(path)

    def abspath(self, path):
        "Join path to cwd and return absolute path"
        return os.path.abspath(
            os.path.join(
                self.cwd,
                path
                )
            )

    def cd(self, path):
        self.cwd = self.abspath(path)
        self.options = self.ls()

    def perform_cd(self, change):
        newindex = change['new']
        newdir = self.options[newindex][1]
        if os.path.isdir(newdir):
            self.cd(newdir)
