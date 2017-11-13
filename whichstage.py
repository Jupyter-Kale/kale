# whichstage.py
# Determine which stages of analysis must be completed for each file

# Stages:
# Zipped -> Unzipped
# Unzipped -> Parsed
# Parsed -> Analyzed

import re
import os
import subprocess
from natsort import natsorted
import numpy as np
import workflow_objects as kale

# DIRs for each stage
zipped_base = "/home/mtsige/Bob/"
unzipped_base = "/home/oge1/lammps/sapphire/analysis/data/"
results_base = "/home/oge1/lammps/sapphire/analysis/results/"
zipped_subdir_list = ["Sub3", "Sub951By50", "Sub951By100", "Quartz"]
unzipped_subdir_list = ["Sphere_Sapphire", "Cyl_Sapphire", "Sphere_Quartz"]

# Zipped/Prezipped files to ignore
prezipped_ignore_list = []
zipped_ignore_list = [
    'Sub3/50A/atom2',
    'Sub3/50A/Repeat/atom1',
    'Sub951By50/atom1'
]

# Name changes from zipped/prezipped to unzipped
# Regex is okay.
# source: destination
# Applied in order
name_changes = [
    ['Quartz', 'Sphere_Quartz'],
    ['Sub3', 'Sphere_Sapphire'],
    ['Sphere_Sapphire/50A/Repeat/atom2', r'Sphere_Sapphire/50A/atom2'],
    ['(Cyl[0-9]{2,3}A)/New', r'\1_New'],
    [r'Sub951By(50|100)/(.*?Cyl[0-9]{2,3}A)', r'Cyl_Sapphire/\2_\1']
]

reverse_name_changes = [
    [r'Cyl_Sapphire/(.*?Cyl[0-9]{2,3}A)_(50|100)', r'Sub951By\2/\1'],
    ['(Cyl[0-9]{2,3}A)_New', r'\1/New'],
    ['Sphere_Sapphire/50A/atom2', 'Sphere_Sapphire/50A/Repeat/atom2'],
    ['Sphere_Sapphire', 'Sub3'],
    ['Sphere_Quartz', 'Quartz']
]

zipped_path_list = [
    os.path.join(zipped_base, subdir)
    for subdir in zipped_subdir_list
]

unzipped_path_list = [
    os.path.join(unzipped_base, subdir)
    for subdir in unzipped_subdir_list
]

results_path_list = [
    os.path.join(results_base, subdir)
    for subdir in unzipped_subdir_list
]

def listdiff(list1, list2):
    "Return list of items in list1, but not in list2."
    return natsorted(list(set(list1) - set(list2)))

def listunion(list1, list2):
    "Return list of items in list1 or list2."
    return natsorted(list(set(list1).union(set(list2))))

def listintersection(list1, list2):
    "Return list of items in list1 and list2."
    return natsorted(list(set(list1).intersection(set(list2))))

def search_path(path, regex):
    "Search a single directory with regex"
    return (
        subprocess.check_output(
            "find {} | egrep '{}'".format(path, regex),
            shell=True
        )
        .decode()
        .split()
    )

def search_path_list(path_list, regex):
    "Search multiple directories with regex"
    files = []
    for path in path_list:
        try:
            files.extend(search_path(path, regex))
        except subprocess.CalledProcessError:
            # Error is raised if search results are empty
            pass

    return files

def find_prezipped():
    "Just simulated, not yet zipped."
    regex = r"atom[0-9]{1,3}$"
    files = search_path_list(zipped_path_list, regex)
    partnames = [
        os.path.relpath(filename, zipped_base)
        for filename in files
    ]
    partnames = [
        part for part in partnames
        if part not in prezipped_ignore_list
    ]
    return natsorted(partnames)

def find_zipped():
    "Simulated and zipped."
    regex = r"atom[0-9]{1,3}\.bz2$"
    files = search_path_list(zipped_path_list, regex)
    partnames = [
        os.path.relpath(filename, zipped_base).replace('.bz2','')
        for filename in files
    ]
    partnames = [
        part for part in partnames
        if part not in zipped_ignore_list
    ]
    return natsorted(partnames)

def find_unzipped():
    "Unzipped into new dir"
    regex = r"atom[0-9]{1,3}$"
    files = search_path_list(unzipped_path_list, regex)
    partnames = [
        os.path.relpath(filename, unzipped_base)
        for filename in files
    ]
    return natsorted(partnames)

def find_parsed():
    "Parsed"
    regex = r"calculated.txt"
    files = search_path_list(results_path_list, regex)
    partnames = [
        (
            os.path.relpath(filename, results_base)
            .replace('/calculated.txt','')
        )
        for filename in files
    ]
    return natsorted(partnames)

def find_analyzed():
    "Analyzed"
    regex = r"avgStepData.txt"
    files = search_path_list(results_path_list, regex)
    partnames = [
        (
            os.path.relpath(filename, results_base)
            .replace('/avgStepData.txt','')
        )
        for filename in files
    ]
    return natsorted(partnames)

def apply_name_changes(pathlist):
    newlist = pathlist[:]
    for i, path in enumerate(pathlist):
        for pre, post in name_changes:
            newlist[i] = re.sub(pre, post, newlist[i])

    return newlist

def undo_name_changes(pathlist):
    newlist = pathlist[:]
    for i, path in enumerate(pathlist):
        for pre, post in reverse_name_changes:
            newlist[i] = re.sub(pre, post, newlist[i])

    return newlist

def whichstage():
    """
    Prezipped or zipped -> unzipped -> parsed -> analyzed
    """

    prezipped = find_prezipped()
    prezipped_nc = apply_name_changes(prezipped)
    zipped = find_zipped()
    zipped_nc = apply_name_changes(zipped)
    unzipped = find_unzipped()
    parsed = find_parsed()
    analyzed = find_analyzed()

    # Test reverse_name_changes
    pz_1_np = np.array(prezipped)
    pz_2_np = np.array(undo_name_changes(prezipped_nc))
    whichdiff = (pz_1_np != pz_2_np)

    z_1_np = np.array(zipped)
    z_2_np = np.array(undo_name_changes(zipped_nc))
    whichdiff = (z_1_np != z_2_np)

    at_least_parsed = listunion(
        parsed,
        analyzed
    )
    at_least_unzipped = listunion(
        unzipped,
        at_least_parsed
    )

    needs_analyzed = listdiff(
        parsed,
        analyzed
    )

    needs_parsed = listdiff(
        unzipped, 
        at_least_parsed
    )

    needs_unzipped = listdiff(
        zipped_nc, 
        at_least_unzipped
    )

    needs_linked = listdiff(
        prezipped_nc, 
        at_least_unzipped
    )

    print("Needs Linked:")
    print(needs_linked)
    print()

    print("Needs Unzipped:")
    print(needs_unzipped)
    print()

    print("Needs Parsed:")
    print(needs_parsed)
    print()

    print("Needs Analyzed:")
    print(needs_analyzed)
    print()

    print("Creating workflow.")
    workflow = create_workflow(
        needs_linked,
        needs_unzipped,
        needs_parsed,
        needs_analyzed
    )
    print("Workflow created!")

    return workflow


def create_workflow(needs_linked, needs_unzipped, needs_parsed, needs_analyzed):
    workflow = kale.Workflow('Droplet Workflow')

    condition = lambda partname: True #'Cyl_Sapphire' in partname and 'Cyl20A' not in partname

    for partname in needs_linked:
        if condition(partname):
            create_link_sequence(partname, workflow)
    for partname in needs_unzipped:
        if condition(partname):
            create_unzip_sequence(partname, workflow)
    for partname in needs_parsed:
        if condition(partname):
            create_parse_sequence(partname, workflow)
    for partname in needs_analyzed:
        if condition(partname):
            create_analyze_sequence(partname, workflow)

    return workflow


def create_unzip_sequence(partname, workflow):
    unzip_task = create_unzip_task(partname, workflow)
    parse_task = create_parse_task(
        partname, workflow,
        dependency=unzip_task
    )
    analyze_task = create_analyze_task(
        partname, workflow,
        dependency=parse_task
    )

def create_link_sequence(partname, workflow):
    link_task = create_link_task(partname, workflow)
    parse_task = create_parse_task(
        partname, workflow,
        dependency=link_task
    )
    analyze_task = create_analyze_task(
        partname, workflow,
        dependency=parse_task
    )

def create_parse_sequence(partname, workflow):
    parse_task = create_parse_task(partname, workflow)
    analyze_task = create_analyze_task(
        partname, workflow,
        dependency=parse_task
    )
    pass

def create_analyze_sequence(partname, workflow):
    analyze_task = create_analyze_task(partname, workflow)
    return [analyze_task]

def create_link_task(partname, workflow, dependency=None):

    prezipped_name = undo_name_changes([partname])[0]
    source = os.path.join(
        zipped_base,
        prezipped_name
    )
    dest = os.path.join(
        unzipped_base,
        partname
    )
    command = "mkdir -p {dirname} && ln -s {source} {dest}"

    task = kale.CommandLineTask(
        name="link_{partname}",
        command=command,
        tags=['link'],
        node_property='node',
        params=dict(
            source=source,
            dest=dest,
            partname=partname,
            dirname=os.path.dirname(dest)
        )
    )

    if dependency is None:
        dependencies = []
    else:
        dependencies = [dependency]

    workflow.add_task(task, dependencies)
    return task

def create_unzip_task(partname, workflow, 
        dependency=None, num_cores=1):

    zipped_name = undo_name_changes([partname])[0]
    source = os.path.join(
        zipped_base,
        zipped_name
    )
    dest = os.path.join(
        unzipped_base,
        partname
    )
    command = "mkdir -p {dirname} && pbzip2 -n {num_cores} -cdk {source}.bz2 > {dest}"

    task = kale.CommandLineTask(
        name="unzip_{partname}",
        command=command,
        tags=['unzip'],
        node_property='node',
                params=dict(
            num_cores=num_cores,
            source=source,
            dest=dest,
            partname=partname,
            dirname=os.path.dirname(dest)
        )
    )

    if dependency is None:
        dependencies = []
    else:
        dependencies = [dependency]

    workflow.add_task(task, dependencies)
    return task

def create_parse_task(partname, workflow, dependency=None):
    source = os.path.join(
        unzipped_base,
        partname
    )
    dest = partname

    script = "/home/oge1/lammps/sapphire/analysis/exec/parse.sh" 
    command = "{script} {source} {dest}"

    task = kale.CommandLineTask(
        name="parse_{partname}",
        command=command,
        tags=['parse'],
        node_property='node',
                params=dict(
            source=source,
            dest=dest,
            partname=partname,
            script=script
        )
    )

    if dependency is None:
        dependencies = []
    else:
        dependencies = [dependency]

    workflow.add_task(task, dependencies)
    return task

def create_analyze_task(partname, workflow, dependency=None):
    source = os.path.join(
        results_base,
        partname,
        'calculated.txt'
    )
    dest = partname

    script = "/home/oge1/lammps/sapphire/analysis/exec/analyze.sh" 
    command = "{script} {source} {dest} polarScatter"

    task = kale.CommandLineTask(
        name="analyze_{partname}",
        command=command,
        tags=['analyze'],
        node_property='node',
                params=dict(
            source=source,
            dest=dest,
            partname=partname,
            script=script
        )
    )

    if dependency is None:
        dependencies = []
    else:
        dependencies = [dependency]

    workflow.add_task(task, dependencies)
    return task

if __name__ == '__main__':
    whichstage()

