# Oliver Evans
# August 7, 2017

class LammpsSim(object):
    def __init__(self, lammpstrj_path):
        self.lammpstrj_path = lammpstrj_path
        self.file_handle = open(lammpstrj_path, 'r')
        self.num_atoms = self.get_num_atoms()
        self.current_step_num = 0
    
    def __enter__(self):
        """
        Allows for using `with LammpsSim(...) as sim`
        Except this doesn't work if you want to continue reading the file
        after the cell is finished executing
        """
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
    
    def close(self):
        try:
            self.file_handle.close()
        except:
            pass
        
    def index_timesteps(self, max_steps=100):
        """
        Read whole file, and save position of each timestep in file
        for easier seeking.
        
        Set max_steps to limit the number of timesteps read
        """
        
        # List of positions of timesteps
        self.step_index = []
        # Keep track of number of steps
        self.num_steps = 0
        
        # Seek to beginning of file
        self.file_handle.seek(0)
        
        # Loop through steps until EOF or max_steps
        for step_num in range(max_steps):
            
            # Save position
            file_pos = self.file_handle.tell()

            # Read header
            header = [self.file_handle.readline() for i in range(9)]
            # Check for EOF, in which case '' will appear in list
            # (as opposed to '\n' for a legitimate blank line)
            if '' in header:
                break
            
            # Number of atoms in this timestep
            num_atoms = int(header[3])
            for atom_num in range(num_atoms):
                # Check only for EOF
                if self.file_handle.readline() == '':
                    break
            
            # If reading atom info ended early, then the file is finished,
            # so don't try to read the next timestep
            if atom_num < num_atoms-1:
                break
                
            # If whole timestep was read sucessfully, save timestep
            self.step_index.append(file_pos)
            self.num_steps += 1
    
    def get_num_atoms(self):
        self.file_handle.seek(0)
        
        # Read first 4 lines
        # (num_atoms is on line 4)
        for line_num in range(4):
            line = self.file_handle.readline()
        
        return int(line)
    
    def seek_to_step(self, step_num):
        """
        Seek to step step_num
        Must have already run index_timesteps.
        """
        self.file_handle.seek(self.step_index[step_num])

    # Parse lammpstrj
    def read_step(self):
        """
        Read one timestep from lammps trajectory file
        Pass a Python file object pointing to the first line
        of the timestep
        (which reads 'ITEM: TIMESTEP')
        """

        # Read header - first 9 lines
        header_len = 9
        try:
            header = [
                self.file_handle.readline() 
                for x in range(header_len)
            ]
        except StopIteration as err:
            print("End of file reached.")
            return

        # Number of atoms
        num_atoms = int(header[3])
        # Box bounds
        bnds = np.array([list(map(float,header[i].split())) for i in range(5,8)]).T

        # Read data
        raw_data = [
            self.file_handle.readline()[:-1] 
            for x in range(num_atoms)
        ]

        # Convert to DataFrame
        column_headers = header[-1].split()[2:]

        string_buffer = io.StringIO('\n'.join(raw_data))
        step_data = pd.read_csv(string_buffer,
                                delimiter=' ',
                                names=column_headers,
                                index_col=False)

        # Generate actual position given scaled position and image numbers
        converted_data = self.calc_positions(step_data,num_atoms,bnds)
        return converted_data

    @staticmethod
    def calc_positions(step_data,num_atoms,bnds):
        scaled_pos = step_data.loc[:,['xs','ys','zs']].values
        im = step_data.loc[:,['ix','iy','iz']].values

        # Separate upper & lower bounds
        lo_bnd, hi_bnd = np.array(bnds)

        real_pos = (scaled_pos + im) * (hi_bnd - lo_bnd) + lo_bnd


        # Combine atom id & type with position
        combined_arr = np.hstack([
            step_data.loc[:,['id','type']].values,
            real_pos
        ])

        headers = ['id','type','x','y','z']
        converted_data = pd.DataFrame(combined_arr,
                                      columns=headers,
        )

        # Enforce appropriate dtypes
        # https://stackoverflow.com/questions/25610592/how-to-set-dtypes-by-column-in-pandas-dataframe
        dtypes = {
            'id': int,
            'type': int,
            'x': float,
            'y': float,
            'z': float
        }
        for col_name, dtype in dtypes.items():
            converted_data[col_name] = converted_data[col_name].astype(dtype)

        return converted_data
    
    @staticmethod
    def lammps_scatter(lammps_df, p3_fig):
        """
        Create 3d scatter from lammps input data
        """

        colors = ['red','green','blue','yellow',
                  'orange','purple','black','brown']
        markers = ['sphere','diamond','box','arrow']

        # All combinations of colors and markers
        # One per atom type
        cm_pairs = it.product(markers,colors)

        # Identify all atom types
        atom_types = lammps_df.loc[:,'type'].unique()
        num_types = len(atom_types)

        # Count number of scatters already plotted in figure
        num_scatters = len(p3_fig.scatters)

        # Note that LAMMPS counts atom types from 1,
        # whereas Python counts indices from 0.

        # Plot each with a different color/marker pair
        for atom_type, cm_pair in zip(atom_types, cm_pairs):
            
            print("Type {}/{}".format(atom_type, num_scatters))

            # Extract atom positions for this type
            pos = lammps_df.query('type == {}'.format(atom_type))
            # Sort atoms by id to retain identity
            # Allows for sensible visual transition between frames
            pos = pos.sort_values('id')

            # Get marker and color
            marker, color = cm_pair

            # Types which have already been plotted
            # - update plot
            if atom_type <= num_scatters:
                
                print("Updating atom type {}".format(atom_type))
                
                # Subtract one from LAMMPS atom type
                # to get Python list index
                indx = atom_type - 1

                # Extract scatter for this atom type
                sct = p3_fig.scatters[indx]

                # Update positions
                sct.x = pos['x']
                sct.y = pos['y']
                sct.z = pos['z']
                
                log_msg('test', 'Update positions for type {}'.format(atom_type))

            # Types which have not yet been plotted
            # - create new plot
            else:
                print("Creating atom type {}".format(atom_type))
                # Create a new scatter
                p3.scatter(
                    x=pos['x'], 
                    y=pos['y'], 
                    z=pos['z'],
                    marker=marker,
                    color=color
                )
    
    def update_timestep(self, step_num):
        print("Update to step {}".format(step_num))
        log_msg('test', 'Update to step {}'.format(step_num))
        self.seek_to_step(step_num)
        self.step_num = step_num
        self.lammps_df = self.read_step()
        self.lammps_scatter(self.lammps_df, self.fig)
    
    def plot_steps(self):
        """
        Main function to call to create plot & timestep widget
        Assuming static file (doesn't listen for new data)
        """
        
        self.fig = p3.figure(controls=None)
        #self.fig.camera_control = 'orbit'
        
        self.seek_to_step(0)
        lammps_df = self.read_step()
        self.lammps_scatter(lammps_df, self.fig)
        
        self.step_slider = ipw.IntSlider(
            min=0, 
            max=self.num_steps-1, 
            value=0,
            description='Timestep'
        )
        
        ipw.interactive(
            self.update_timestep, 
            step_num=self.step_slider
        )
        
        container = ipw.VBox([
            self.fig,
            self.step_slider
        ])
        
        return container
        

if __name__ == '__main__':

	# Test that seek_to_step is working properly
	@ipw.interact(step_num=(0,100))
	def test_seek(step_num):
		sim.seek_to_step(step_num)
		header = [sim.file_handle.readline() for i in range(9)]
		
		print('Step = {}'.format(step_num))
		print()
		
		print(''.join(header))
		

	fname = "/home/oliver/academic/research/graphene/two_sheets_ripple.lammpstrj"
	with open(fname) as file_handle:
		for i in range(11):
			# Read 10 steps at a time
			for j in range(10):
				lammps_df = read_step(file_handle)
			lammps_scatter(lammps_df, fig)
			time.sleep(1)


