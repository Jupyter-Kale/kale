# Oliver Evans
# August 7, 2017

from kale.workflow_objects import *

cyl_wf = Workflow('Cylinder Workflow')

base_dir="/home/oge1/lammps/sapphire/analysis"
zipped_dir="/home/mtsige/Bob/"
unzipped_dir=os.path.join(base_dir, "data")

num_parts = 2

group = "Cyl_Sapphire"
sim = "Cyl20A_100"

for part_num in range(1,num_parts+1):
    zipped_path = os.path.join(
        zipped_dir,
        "Sub951By100/Cyl20A/atom{}.bz2".format(part_num)
    )
    filename = "{group}/{sim}/atom{part}".format(
        group=group,
        sim=sim,
        part=part_num
    )
    unzip_task = CommandLineTask(
        name="unzip_{group}_{sim}_atom{part}",
        command="pbzip2 -p{num_cores} -cdk {input_file} > {output_file}",
        output_files = ["{output_file}"],
        input_files = ["{input_file}"],
        tags = ["unzip"],
        params = dict(
            base_dir=base_dir,
            input_file=zipped_path,
            output_file=os.path.join(
                unzipped_dir,
                filename
            ),
            group=group,
            sim=sim,
            part=part_num,
            num_cores=4
        )
    )
    cyl_wf.add_task(unzip_task)

    parse_task = CommandLineTask(
        name="parse_{group}_{sim}_atom{part}",
        command="{base_dir}/exec/parse.sh {input_dir} {output_dir}",
        params = dict(
            base_dir=base_dir,
            input_dir=os.path.join(
                unzipped_dir,
                filename
            ),
            output_dir=filename,
            group=group,
            sim=sim,
            part=part_num
        )
    )
    cyl_wf.add_task(
        parse_task,
        dependencies=[unzip_task]
    )

    analyze_task = CommandLineTask(
        name="analyze_{group}_{sim}_atom{part}",
        command="{base_dir}/exec/analyze.sh {input_dir} {output_dir} polarScatter",
        params = dict(
            base_dir=base_dir,
            input_dir=os.path.join(
                unzipped_dir,
                filename
            ),
            output_dir=filename,
            group=group,
            sim=sim,
            part=part_num
        )
    )
    cyl_wf.add_task(
        analyze_task,
        dependencies=[parse_task]
    )


