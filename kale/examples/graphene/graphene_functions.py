# Oliver Evans
# August 7, 2017


def gen_lammps_data(job_name, bounds, sheet_dict_list, job_dir):
    """
    Generate graphene sheets and write in LAMMPS format.
    bounds is a 3x2 array of 3D simulation bounds
    sheet_dict_list is a list of dicts, each containing:
    - loc (3-array)
    - nx (# of hexagons in x direction)
    - ny (# of hexagons in y direction)
    - bond_length
    - pair_length
    - bond_strength
    - pair_strength
    - angle_strength
    """

    # Simulation parameters
    sim = lg.Simulation(bounds)
    
    for sheet_dict in sheet_dict_list:
        
        nx = sheet_dict['nx']
        ny = sheet_dict['ny']
        loc = sheet_dict['loc']
        bond_length = sheet_dict['bond_length']
        pair_length = sheet_dict['pair_length']
        bond_strength = sheet_dict['bond_strength']
        pair_strength = sheet_dict['pair_strength']
        angle_strength = sheet_dict['angle_strength']

        sheet = lg.GrapheneSheet(sim, nx, ny)
        
        sheet.set_loc(*loc)

        sheet.set_bond_length(bond_length)
        sheet.set_bond_strength(bond_strength)

        sheet.set_angle_measure(120)
        sheet.set_angle_strength(angle_strength)

        sheet.set_pair_length(pair_length)
        sheet.set_pair_strength(pair_strength)

    # Save
    output_loc = os.path.join(job_dir, '{job_name}.data'.format(job_name=job_name))
    sim.write(output_loc)
    
    print("LAMMPS data file written to {}.data".format(job_name))


def gen_lammps_input(job_name, nsteps, dump_freq, job_dir,
    px=1, py=1, pz=1,
    lammps_template_file="/global/homes/o/oevans/graphene/lammps_template.in"):
    "Generate LAMMPS input script for graphene simulation, varying a few io parameters"
    
    # Read template file
    with open(lammps_template_file) as template_handle:
        template_str = template_handle.read()
    
    # Anonymous function to replace all parameters in string
    replace_params = lambda s: s.format(
        job_name=job_name,
        job_dir=job_dir,
        px=px,
        py=py,
        pz=pz,
        nsteps=nsteps,
        dump_freq=dump_freq
    )
    
    lammps_str = replace_params(template_str)
    
    output_name = "{}.in".format(job_name)
    
    replaced_output_name = os.path.join(job_dir, replace_params(output_name))
    
    # Write output file
    with open(replaced_output_name, 'w') as out_handle:
        out_handle.write(lammps_str)  
        
    print("LAMMPS input file written to {}".format(replaced_output_name))

def gen_slurm_script(job_name, nodes=1, cores=32, partition="debug", job_time="10:00",
                    batch_template_file="/global/homes/o/oevans/graphene/batch_template",
                    job_dir="/global/homes/o/oevans/graphene/{job_name}"):
    "Generate slurm script for a job, directing output to a new folder"
    
    # Read template file
    with open(batch_template_file) as template_handle:
        template_str = template_handle.read()
        
    
    # Anonymous function to replace all parameters in string
    replace_params = lambda s: s.format(
        job_name=job_name,
        nodes=nodes,
        cores=cores,
        partition=partition,
        job_time=job_time,
        batch_template_file=batch_template_file,
        job_dir=job_dir
    )
    
    # Replace template strings
    batch_str = replace_params(template_str)
    
    output_name = "{}.batch".format(job_name)
    
    # Determine name of output batch file
    # Can handle parameter replacement
    replaced_output_name = os.path.join(job_dir, replace_params(output_name))
    
    # Write output file
    with open(replaced_output_name, 'w') as out_handle:
        out_handle.write(batch_str)
    
    
    print("SLURM batch script written to {}".format(replaced_output_name))
        
def newt_submit(auth_widget, jobfile="/global/homes/o/oevans/graphene/test_job.batch", queue='cori'):
    "Submit a batch job to an HPC queue using NEWT."
    
    session = auth_widget._session
    url = "https://newt.nersc.gov/newt/queue/{queue}".format(queue=queue)
    request = session.post(url, {'jobfile': jobfile})
    
    print("Job submitted from '{}'".format(jobfile))
    #return disp.HTML(auth_widget._request_json_to_html(request))
    return request
    
# Wrap all above
def simulate_graphene(job_name, auth_widget, sheet_dict_list, nsteps, 
                      dump_freq, base_dir, bounds, px=1, py=1, 
                      pz=1, nodes=1, queue="cori", partition='debug', job_time="10:00",
                      template_dir = "/global/homes/o/oevans/graphene/"):
    """Generate LAMMPS data and input files and SLURM job submission script
    for graphene simulation, and submit to SLURM queue."""
    
    # Make directory if necessary 
    job_dir = os.path.join(base_dir, job_name)
    os.makedirs(job_dir, exist_ok=True)
    
    cores = px * py * pz
    jobfile = os.path.join(job_dir, job_name+".batch")
    
    gen_lammps_data(job_name, bounds, sheet_dict_list, job_dir)
    
    gen_lammps_input(job_name, nsteps, dump_freq, 
        px=px, py=py, pz=pz, job_dir=job_dir,
        lammps_template_file=template_dir+"lammps_template.in")
    
    gen_slurm_script(job_name=job_name, nodes=nodes, cores=cores, partition=partition, job_time=job_time,
        batch_template_file=template_dir+"batch_template",job_dir=job_dir)
    
    request = newt_submit(auth_widget, jobfile=jobfile, queue=queue)
    
    print("Simulation submitted.")
    print(request.json())
