# Oliver Evans
# August 7, 2017

from workflow_objects import *

droplet_wf = Workflow('Droplet Workflow')

droplet_wf.readme = r"""
<h3>MD Simulations of Nanodroplet Wetting Dynamics</h3>

<br>

We investigate the interfacial interactions between nanoscopic water droplets
on the order of 20 - 100 $\overset{\lower.5em\circ}{\mathrm{A}}$ and atomically flat substrates.
In particular, we observe the formation of a monolayer, calculate its rate
of growth, and devise a predictive geometric model for the phenomenon.

<video width="300px" src="movie/mesfin.mp4" type="video/mp4" controls>
    Video not supported.
</video>

<br><br>

We find that $r_m = \alpha t^\beta$ is an accurate model for monolayer radius
for values of $\alpha$ and $\beta$ which can be determined from the base radius.

<br><br>



<br><br>


This workflow does the following:
<ol>
    <li>Create substrate and droplets (C++, ROOT, Python)
    <li>Combine substrate and droplets (Perl)
    <li>Run simulation (LAMMPS on cluster)
    <li>Parse LAMMPS output(awk, C++)
    <li>Calculate molecular properties (C++, ROOT)
        <ul>
            <li>Velocity
            <li>Orientation
            <li>etc.
        </ul>
    <li>Calculate droplet properties (C++, ROOT_)
        <ul>
            <li>Bulk radius
            <li>Monolayer Radius
            <li>etc.
        </ul>
    <li>Combine results for each droplet size (Bash)
    <li>Combine results from all simulations (Bash)
    <li>Define model and calibrate to simulation data (Jupyter Notebook, Python)
</ol>
"""


# Radius of droplets (Angstroms)
droplet_radii = [20,30,40,50,60,100]
# Shape of droplets
shape = 'sphere'
# Base directory for computations
base_dir = '$SCRATCH/droplet'

# Number of substrate images in each dimension
nx, ny = 10, 10

# Number of parts (dump files) per simulation
parts_per_sim = 3

# Generate substrate
gen_mica_task = CommandLineTask(
    name='gen_mica_{nx}x{ny}',
    command='{base_dir}/gen_droplet/scripts/gen_mica.sh {nx} {ny} {out_file}',
    output_files = [
        "{out_file}"
    ],
    params=dict(
        base_dir=base_dir,
        nx=nx,
        ny=ny,
        out_file="{base_dir}/gen_droplet/lammps_data/mica_{nx}x{ny}.data".format(
            base_dir=base_dir,
            nx=nx,
            ny=ny
        )
    )
)
droplet_wf.add_task(gen_mica_task)

# Loop over droplet sizes
for radius in droplet_radii:
    # Create droplet
    gen_droplet_task = CommandLineTask(
        name="gen_droplet-{radius}A",
        command="{base_dir}/gen_droplet/bin/waterdroplet_tip4p_new.out {radius} {shape}",
        output_files = [
            "{out_file}"
        ],
        params=dict(
            base_dir=base_dir,
            radius=radius,
            shape=shape,
            out_file="{base_dir}/gen_droplet/dump/droplet_{radius}A.lammpstrj".format(
                base_dir=base_dir,
                radius=radius
            )
        )
    )      
    droplet_wf.add_task(gen_droplet_task)
    
    # Combine with substrate
    combine_task = CommandLineTask(
        name="combine-{radius}A",
        command="{base_dir}/gen_droplet/scripts/combine_sub_strip.pl {substrate} {film} {gap}",
        input_files = [
            "{substrate}",
            "{film}"
        ],
        output_files = [
            "{base_dir}/gen_droplet/lammps_data/droplet_on_mica-{radius}A.data"
        ],
        params=dict(
            base_dir=base_dir,
            radius=radius,
            substrate=gen_mica_task.output_files[0],
            film=gen_droplet_task.output_files[0],
            gap=radius,
        )
    )
    droplet_wf.add_task(
        combine_task,
        dependencies=[
            gen_mica_task,
            gen_droplet_task
        ]
    )
    
    simulate_task = BatchTask(
        name="simulate-{radius}A",
        batch_script="{base_dir}/sub_scripts/simulate_{radius}A.batch",
        input_files = [
            combine_task.output_files[0],
            "{base_dir}/lammps_scripts/simulate_{radius}A.batch"
        ],
        output_files = [
            "{base_dir}/data/{radius}A/atom"+str(part)
            for part in range(1,parts_per_sim+1)
        ],
        num_cores=parts_per_sim,
        params=dict(
            base_dir=base_dir,
            radius=radius,
        )
    )
    droplet_wf.add_task(
        simulate_task,
        dependencies=[combine_task]
    )
    
    # Analyze each part independently
    for part in range(1,parts_per_sim+1):
        parse_task = CommandLineTask(
            name='parse-{radius}A_atom{part}',
            command='{base_dir}/exec/parse.sh {infile} {outfile}',
            input_files = ["{infile}"],
            output_files = ["{outfile}"],
            params=dict(
                base_dir=base_dir,
                radius=radius,
                part=part,
                infile=simulate_task.output_files[part-1],
                outfile="{base_dir}/results/{radius}A/waters.txt".format(
                    base_dir=base_dir,
                    radius=radius
                )
            )
        )
        droplet_wf.add_task(
            parse_task,
            dependencies=[simulate_task]
        )
        
        analyze_readme = r"""
        <h3>ROOT Preliminary Post-Simulation Analysis</h3>
        <h5>{radius}$\overset{{\lower.5em\circ}}{{\mathrm{{A}}}}$, {part} ns</h5>
        <br>

        <img width="300px" src="img/hist.png" />

        <br><br>

        We calculate several properties here, both of each molecule and of the
        droplet as a whole.
        <br><br>

        In order to calculate the edge of the droplet given atomic postitions,
        we calculate density in discrete bins, and perform fits along rows and columns
        in the $r-z$ plane. We model the decay of density along these bins with
        <br><br>
        \begin{{equation}}
            \displaystyle \rho(r) = \frac{{\rho_w}}{{2}} \tanh(1-r) + 1
        \end{{equation}}
        <br>
        This proves to be an accurate method for this fit.
        <br><br>

        We also calculate several quantities and generate movies during this step.
        """
        analyze_task = CommandLineTask(
            name='analyze-{radius}A_atom{part}',
            command='{base_dir}/exec/analyze.sh {infile} {outfile}',
            input_files = ["{infile}"],
            output_files = ["{outfile}"],
            readme=analyze_readme,
            params=dict(
                base_dir=base_dir,
                radius=radius,
                part=part,
                infile=parse_task.output_files[0],
                outfile="{base_dir}/results/{radius}A/calculated.txt".format(
                    base_dir=base_dir,
                    radius=radius
                )
            )
        )
        
        droplet_wf.add_task(
            analyze_task,
            dependencies=[parse_task]
        )
    
    combine_parts_task = CommandLineTask(
        name='combine_parts-{radius}A',
        command='{base_dir}/results/combineParts.sh {radius}A',
        input_files = [
            "{base_dir}/results/{radius}A/atom"+str(part)+"/calculated.txt"
            for part in range(1,parts_per_sim+1)
        ],
        output_files=["{base_dir}/results/{radius}A/combined.txt"],
        params=dict(
            base_dir=base_dir,
            radius=radius,
        )
    )
    droplet_wf.add_task(
        combine_parts_task,
        dependencies=[droplet_wf.get_task_by_name(
            'analyze-{radius}A_atom{part}'.format(
                radius=radius,
                part=part
            )
        )
        for part in range(1,parts_per_sim+1)
        ]
    )
    
combine_sims_task = CommandLineTask(
    name='combine_sims',
    command='{base_dir}/results/combineSims.sh',
    input_files = [
        "{base_dir}/results/"+str(radius)+"A/combined.txt"
        for radius in droplet_radii
    ],
    output_files=["{base_dir}/results/allResults.txt"],
    params=dict(base_dir=base_dir)
)
droplet_wf.add_task(
    combine_sims_task,
    dependencies=[
        droplet_wf.get_task_by_name(
            'combine_parts-{radius}A'.format(
                radius=radius
            )
        )
        for radius in droplet_radii
    ]
)
analysis_notebook_task = NotebookTask(
    name='analysis_notebook',
    interactive=True,
)
droplet_wf.add_task(
    analysis_notebook_task,
    dependencies=[combine_sims_task]
)
