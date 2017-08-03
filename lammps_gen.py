# lammps_gen.py
import numpy as np
#import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
import itertools as it

# Whole simulation box
class Simulation(object):
    def __init__(self,bounds):
        try:
            if bounds.shape != (3,2):
                raise TypeError("bounds should be a 3x2 numpy array")
        except(AttributeError):    
            raise TypeError("bounds should be a 3x2 numpy array")

        # Box dimensions
        self.bounds = bounds

        # Molecule dict
        # values: Molecule objects
        # keys: Molecule object ids given by id()
        self.molecules = []

    # Write LAMMPS data file
    def write(self,out_loc = None):

        # Output file
        if out_loc == None:
            out_loc = 'graphene_{}x{}.data'.format(
                    self.molecules[0].hex_n,self.molecules[0].hex_m)

        #File object
        with open(out_loc,'w') as out_file:

            #Initial comment
            out_file.write('# {} x {} Graphene Sheet\n'.format(
                self.molecules[0].hex_n,self.molecules[0].hex_m)) 

            #Counters
            atom_id=1
            atomType=1
            molNum=1

            #Headers section
            out_file.write("{:4d} atoms\n".format(
                int(sum([mol.num_atoms for mol in self.molecules]))))
            out_file.write("{:4d} bonds\n".format(
                int(sum([mol.num_bonds for mol in self.molecules]))))
            out_file.write("{:4d} angles\n".format(
                int(sum([mol.num_angles for mol in self.molecules]))))
            #out_file.write("{} angles\n".format(nAngles))
            out_file.write("\n")
            out_file.write("{:4d} atom types\n".format(len(self.molecules)))
            out_file.write("{:4d} bond types\n".format(len(self.molecules)))
            out_file.write("{:4d} angle types\n".format(len(self.molecules)))
            #out_file.write("1 angle type\n")
            out_file.write("\n")

            #Boundary
            out_file.write("{:7.3f} {:7.3f} xlo xhi\n".format(*self.bounds[0]))
            out_file.write("{:7.3f} {:7.3f} ylo yhi\n".format(*self.bounds[1]))
            out_file.write("{:7.3f} {:7.3f} zlo zhi\n".format(*self.bounds[2]))
            out_file.write("\n")

            #Masses section
            c_mass=12.011
            out_file.write("Masses \n\n")
            for mol_num,mol in enumerate(self.molecules):
                out_file.write("{:4d} {:7.3f} # C\n".format(mol_num+1,c_mass))
            out_file.write("\n")

            #Atoms section
            out_file.write("Atoms # full\n\n")
            for mol in self.molecules:
                mol.write_atoms(out_file)
            out_file.write("\n")

            #Bonds section
            out_file.write("\nBonds\n\n")
            for mol in self.molecules:
                mol.write_bonds(out_file)
            out_file.write("\n")

            #Angles section
            out_file.write("\nAngles\n\n")
            for mol in self.molecules:
                mol.write_angles(out_file)
            out_file.write("\n")

            #Pair Coeff section
            out_file.write("Pair Coeffs # lj/cut/coul/long \n\n")
            #for pair in it.combinations_with_replacement(
            #        range(len(self.molecules)),2):
            #    out_file.write("{:4d} {:4d} {:7.3f} {:7.3f} # C-C \n".format(
            #        pair[0]+1,pair[1]+1,eps,sig))
            for mol_num,mol in enumerate(self.molecules):
                out_file.write("{:4d} {:7.3f} {:7.3f} # C-C \n".format(
                    mol_num+1,mol.pair_eps,mol.pair_sig))
            out_file.write("\n")

            #Bond Coeff section
            out_file.write("Bond Coeffs # harmonic \n\n")
            for mol_num,mol in enumerate(self.molecules):
                out_file.write("{:4d} {:7.3f} {:7.3f} # C-C".format(mol_num+1,mol.bond_strength,mol.bond_length))
                out_file.write("\n")
            out_file.write("\n")

            #Angle Coeff section
            out_file.write("Angle Coeffs # harmonic \n\n")
            for mol_num,mol in enumerate(self.molecules):
                out_file.write("{:4d} {:7.3f} {:7.3f} # C-C".format(mol_num+1,mol.angle_strength,mol.angle_measure))
                out_file.write("\n")
            out_file.write("\n")


            print("LAMMPS input file written to: {}".format(out_loc))



# Molecule, "made up" of atoms
# In practice, they do not contain atom objects, 
# but lists of their properties
class Molecule(object):
    def __init__(self,sim):
        # Simulation in which molecule resides
        self.sim = None
        self.set_sim(sim)

        # Number of atoms
        self.num_atoms = 0

        # Coordinate lists:
        # 1D array of x,y,z coordinates of atoms w/ num_atoms elements
        self.x_coords = np.zeros([0])
        self.y_coords = np.zeros([0])
        self.z_coords = np.zeros([0])

        # Bond list: num_atoms x 2 array
        # each row contains 2 atom ids
        self.bond_list = np.zeros([0,2],dtype=int)

        # Location ((x,y,z) center for translation)
        self.loc = np.zeros(3)

    # Change simulation
    def set_sim(self,sim):
        # Remove molecule from old simulation
        if self.sim != None:
            self.sim.molecules.pop(
                    self.sim.molecules.index(self))

        # Change simulation associated w/ molecule
        self.sim = sim

        if self.sim != None:
            # Add molecule to new simulation
            self.sim.molecules.append(self)

    # Translate molecule
    def translate(self,dx,dy,dz):
        # Translate center
        self.loc += np.array([dx,dy,dz])

        # Translate 
        self.x_coords += dx
        self.y_coords += dy
        self.z_coords += dz

    # Set location relative to origin
    def set_loc(self,x,y,z):
        self.translate(x-self.loc[0],y-self.loc[1],z-self.loc[2])


# Homogeneous molecule, contains only one atom type
class HomoMolecule(Molecule):
    def __init__(self,sim):
        # Call Molecule __init__ first
        super().__init__(sim)

        # Atom properties
        self.atom_type = 1
        self.atom_charge = 0

        # Bond parameters
        self.bond_length = 1
        self.bond_strength = 1

        # Bond parameters
        self.angle_measure = 1
        self.angle_strength = 1

        # Pairwise interactions (eps,sigma)
        self.pair_eps = 1
        self.pair_sig = 1

    # Set atom type
    def set_atom_type(self,atom_type):
        self.atom_type = atom_type

    # Set atom charge
    def set_atom_charge(self,atom_charge):
        self.atom_charge = atom_charge

    # Set bond length
    def set_bond_length(self,bond_length):
        self.bond_length = bond_length

    # Set bond strength
    def set_bond_strength(self,bond_strength):
        self.bond_strength = bond_strength

    # Set angle measure
    def set_angle_measure(self,angle_measure):
        self.angle_measure = angle_measure

    # Set angle strength
    def set_angle_strength(self,angle_strength):
        self.angle_strength = angle_strength

    # Set pair length
    def set_pair_length(self,pair_sig):
        self.pair_sig = pair_sig

    # Set pair strength
    def set_pair_strength(self,pair_eps):
        self.pair_eps = pair_eps

    # Write atom data in LAMMPS format
    # id mol type q x y z cells[0] cells[1] cells[2]
    def write_atoms(self,out_file):
        mol_num = self.sim.molecules.index(self)
        atom_offset = sum([mol.num_atoms for mol in self.sim.molecules[:mol_num]])
        #Loop through all unit cells
        for k in range(self.num_atoms):
            q = self.atom_charge
            x = self.x_coords[k]
            y = self.y_coords[k]
            z = self.z_coords[k]
            atom_type = mol_num + 1
            
            #Write file
            out_file.write("{:4d} {:4d} {:4d} {:7.3f} {:7.3f} {:7.3f} {:7.3f}".format(
                k+1+atom_offset,
                mol_num + 1,
                atom_type,q,x,y,z))
            out_file.write("\n")
    
    # Write bond data in LAMMPS format
    # id atom1 atom2
    def write_bonds(self,out_file):
        mol_num = self.sim.molecules.index(self)
        atom_offset = sum([mol.num_atoms for mol in self.sim.molecules[:mol_num]])
        bond_offset = sum([mol.num_bonds for mol in self.sim.molecules[:mol_num]])
        for bond_num,bond in enumerate(self.bond_list):
            bond_type = mol_num + 1
            out_file.write("{:4d} {:4d} {:4d} {:4d}".format(
                bond_num+1+bond_offset,
                bond_type,
                bond[0]+1+atom_offset,
                bond[1]+1+atom_offset))
            out_file.write("\n")

    # Write angle data in LAMMPS format
    # id atom1 atom2 atom3
    def write_angles(self,out_file):
        mol_num = self.sim.molecules.index(self)
        atom_offset = sum([mol.num_atoms for mol in self.sim.molecules[:mol_num]])
        angle_offset = sum([mol.num_angles for mol in self.sim.molecules[:mol_num]])
        for angle_num,angle in enumerate(self.angle_list):
            angle_type = mol_num + 1
            out_file.write("{:4d} {:4d} {:4d} {:4d} {:4d}".format(
                angle_num+1+angle_offset,
                angle_type,
                angle[0]+1+atom_offset,
                angle[1]+1+atom_offset,
                angle[2]+1+atom_offset))
            out_file.write("\n")


# Graphene sheet - hexagonal lattice
# Before translation: centered @ origin, lies in xy-plane
class GrapheneSheet(HomoMolecule):
    def __init__(self,sim,hex_n=3,hex_m=3):
        # Call HomoMolecule __init__ first
        super().__init__(sim)

        # n x m grid of hexagons
        # hex_m: number of hexagon rows
        # cell_m: number of cell rows
        self.hex_n = hex_n  # horizontal size / # of columns (x)
        self.hex_m = hex_m  # vertical size / # of rows (y)

        # Calculate number of cells from number of hexagons
        self.hex2cell()

        # Numbers of atoms per cell
        self.atoms_per_cell = 4
        # Number of bonds per cell
        self.bonds_per_cell = 6
        # Number of angles per cell
        self.angles_per_cell = 12

        # Total number of atoms to be generated:
        # atoms_per_cell * num_cells + n_added_to_right_edge
        self.num_atoms = (self.atoms_per_cell * self.cell_n + 4) * self.cell_m - 2
        # Total number of bonds to be generated
        # bonds_per_cell * num_cells - bonds_not_formed_on_top
        self.num_bonds = (self.bonds_per_cell * self.cell_m - 1) * self.cell_n \
                + 4 * self.cell_m - 3
        # Total number of angles to be generated
        self.num_angles = self.angles_per_cell * self.cell_n * self.cell_m \
                - 4 * self.cell_n + 4 * self.cell_m - 6

        # Set angle measure
        self.angle_measure = 120
            
        # Allocate atom coordinate arrays
        self.x_coords = np.zeros(self.num_atoms)
        self.y_coords = np.zeros(self.num_atoms)
        self.z_coords = np.zeros(self.num_atoms)
        # Allocate bond array
        self.bond_list = np.zeros([self.num_bonds,2],dtype=int)
        # Allocate angle array
        self.angle_list = np.zeros([self.num_angles,3],dtype=int)

        # Generate grid
        self.gen_grid()

    # Calculate number of cells from number of hexagons
    ## CURRENTLY REQUIRES ODD NUMBER OF HEX ROWS ##
    def hex2cell(self):
        self.cell_m = 0
        if self.hex_m % 2 == 1:
            self.cell_m = int((self.hex_m + 1) / 2)
        else:
            raise ValueError("Please give an odd number of rows.")

        # n is the same for cells or hexagons
        self.cell_n = self.hex_n


    # Regenerate grid without changing location
    def regen_grid(self):
        old_loc = self.loc
        self.gen_grid()
        self.loc = np.zeros(3)
        self.set_loc(*old_loc)

    # Set hex_n and hex_m
    def set_size(self,hex_n,hex_m):
        self.hex_n = hex_n
        self.hex_m = hex_m
        self.hex2cell()
        self.regen_grid()

    def get_size(self):
        return (self.hex_n,self.hex_m)

    # Change bond length
    def set_bond_length(self,bond_length):
        self.bond_length = bond_length
        self.regen_grid()
    
    # Convert unit cell image position to atom number
    # i = horizontal cell grid position
    # j = vertical cell grid position
    # k = number of corresponding atom in first cell
    # Returns id of atom k in cell (i,j)
    def grid2id(self,i,j,k):
        # Test for out of bounds
        if i > self.cell_n or i < 0:
            raise ValueError("Column out of bounds")
        elif j >= self.cell_m or j < 0:
            raise ValueError("Row out of bounds")
        elif k >= self.atoms_per_cell or k < 0:
            raise ValueError("Atom number out of bounds")

        # Determine id number based on cases
        # Base case ignoring right edge
        atom_id = (i+j*self.cell_n)*self.atoms_per_cell + k

        # Account for previous edges past first row
        if j > 0:
            atom_id += 4*j - 1

        # Rightmost bin (i=n, edge case) is more complex.
        if i == self.cell_n:
            # atom 0 not included for first row
            if j == 0 and k == 0:
                raise ValueError("Referencing undefined atom: ({},{},{})".format(i,j,k))
            # atom 3 not included for last row
            elif j == self.cell_m - 1 and k == 3:
                raise ValueError("Referencing undefined atom: ({},{},{})".format(i,j,k))
            if j == 0:
                atom_id -= 1
        return atom_id

    # Generate grid, updating atom & bond lists
    def gen_grid(self):
        # Set hex edge length equal to bond length
        l = self.bond_length

        # x coordinates of cell atoms
        x_cell = np.array([np.sqrt(3)/2*l,0,0,np.sqrt(3)/2*l])
        # y coordinates of cell atoms
        y_cell = np.array([0,l/2,3*l/2,2*l])

        # Cell length in each dimension
        x_len = np.sqrt(3)*l
        y_len = 3*l

        # List of bonds between atoms within one cell
        cell_bond_list = np.array([
            [0,1],
            [1,2],
            [2,3]])

        # List of bonds between atoms in horizontally adjacent cells
        # First id is in left cell, second is in right cell
        h_bond_list = np.array([
            [0,1],
            [3,2]])

        # List of bonds between atoms in vertically adjacent cells
        # First id is in lower cell, second is in upper cell
        v_bond_list = np.array([
            [3,0]])

        # List of angles. Each atom is given by a triplet, (i,j,k) denoting
        # its position relative to the cell in question.
        # For example, (1,0,3) is atom 3 in the cell to the right.
        # Each angle is composed of 3 atoms.
        # We have angles_per_cell angles to form per cell
        # This will be a angles_per_cell x 3 x 3 array.
        cell_angle_list = np.array([
            [[0,0,0],[0,0,1],[0,0,2]],
            [[0,0,1],[0,0,2],[0,0,3]],
            [[0,0,2],[0,0,3],[1,0,2]],
            [[0,0,3],[1,0,2],[1,0,1]],
            [[1,0,2],[1,0,1],[0,0,0]],
            [[1,0,1],[0,0,0],[0,0,1]],
            [[0,0,0],[1,0,1],[1,0,0]],
            [[1,0,3],[1,0,2],[0,0,3]],
            [[0,1,0],[0,0,3],[0,0,2]],
            [[1,0,2],[0,0,3],[0,1,0]],
            [[0,1,1],[0,1,0],[0,0,3]],
            [[0,0,3],[0,1,0],[1,1,1]]])

        # Ordered lists of coordinates across all cells
        self.x_coords = np.zeros(self.num_atoms)
        self.y_coords = np.zeros(self.num_atoms)
        self.z_coords = np.zeros(self.num_atoms)

        # Overall atom num. counter
        atom_num = 0

        # Overall bond num. counter
        bond_num = 0

        # Overall angle num. counter
        angle_num = 0

        # Loop through cells
        # along y (among rows)
        for j in range(self.cell_m):
            # along x (within a row)
            for i in range(self.cell_n+1):
                if i != self.cell_n:
                    # Loop through atoms in cell
                    for k_cell in range(self.atoms_per_cell):
                        # Calculate coordinates
                        self.x_coords[atom_num] = x_cell[k_cell] + x_len * i
                        self.y_coords[atom_num] = y_cell[k_cell] + y_len * j

                        # Increment atom counter
                        atom_num += 1

                    # Create bonds within a cell
                    for bond in cell_bond_list:
                        self.bond_list[bond_num] = [
                                self.grid2id(i,j,bond[0]),
                                self.grid2id(i,j,bond[1])]
                        # Increment bond counter
                        bond_num += 1

                    # Create bonds between cells in a row
                    for bond in h_bond_list:
                        self.bond_list[bond_num] = [
                                self.grid2id(i,j,bond[0]),
                                self.grid2id(i+1,j,bond[1])]
                        # Increment bond counter
                        bond_num += 1

                    # Create bonds between cells in a column
                    if j < self.cell_m - 1:
                        for bond in v_bond_list:
                            self.bond_list[bond_num] = [
                                    self.grid2id(i,j,bond[0]),
                                    self.grid2id(i,j+1,bond[1])]
                        # Increment bond counter
                        bond_num += 1

                # Create angles
                for angle in cell_angle_list:
                    # It will try to write one more angle before
                    # realizing it's trying to reference a
                    # non-existent atom
                    if angle_num == self.num_angles:
                        break
                    try:
                        # hatted indices because they're relative
                        # to cell in question, not absolute.
                        # ind counts 3 atoms in angle
                        for ind,atom in enumerate(angle):
                            i_hat,j_hat,k_hat = atom
                            self.angle_list[angle_num][ind] = \
                                    self.grid2id(i+i_hat,j+j_hat,k_hat)
                        # Increment angle counter
                        angle_num += 1
                    except(ValueError):
                        pass

            # Add atom 0 to the right edge for all but first row
            if j != 0:
                self.x_coords[atom_num] = x_cell[0] + x_len * self.cell_n
                self.y_coords[atom_num] = y_cell[0] + y_len * j
                atom_num += 1

            # Add atoms 1 and 2 to the right edge for each row
            self.x_coords[atom_num] = x_cell[1] + x_len * self.cell_n
            self.y_coords[atom_num] = y_cell[1] + y_len * j
            self.x_coords[atom_num+1] = x_cell[2] + x_len * self.cell_n
            self.y_coords[atom_num+1] = y_cell[2] + y_len * j
            # Increment atom counter
            atom_num += 2

            # Add atom 3 to the right edge for all but last row
            if j != self.cell_m - 1:
                self.x_coords[atom_num] = x_cell[3] + x_len * self.cell_n
                self.y_coords[atom_num] = y_cell[3] + y_len * j
                atom_num += 1

            # Create bonds on the edge of each row
            # (within cell)
            for bond in cell_bond_list:
                try:
                    self.bond_list[bond_num] = [
                            self.grid2id(self.cell_n,j,bond[0]),
                            self.grid2id(self.cell_n,j,bond[1])]
                    # Increment bond counter
                    bond_num += 1
                except(ValueError):
                    pass
            # (vertical)
            for bond in v_bond_list:
                try:
                    self.bond_list[bond_num] = [
                            self.grid2id(self.cell_n,j,bond[0]),
                            self.grid2id(self.cell_n,j+1,bond[1])]
                    # Increment bond counter
                    bond_num += 1
                except(ValueError):
                    pass

        # Center sheet at origin
        self.loc = np.array([(
                self.cell_n) * x_len + np.sqrt(3)/2 * self.bond_length,
                self.cell_m * y_len - 2 * self.bond_length,0])/2
        self.set_loc(0,0,0)

    # Plot atoms and bonds
    def plot_atoms(self):
        ax = plt.gca()
        plt.plot(self.x_coords,self.y_coords,'or')

        # Plot bonds
        segs = []
        for bond_num,bond in enumerate(self.bond_list):
            segs.append([
                (self.x_coords[bond[0]],self.y_coords[bond[0]]),
                (self.x_coords[bond[1]],self.y_coords[bond[1]])])

        # Annotate each atom
        for atom_num in range(self.num_atoms):
            plt.annotate('{:4d}'.format(atom_num),
                    xy=(self.x_coords[atom_num],
                        self.y_coords[atom_num]))
        lc = LineCollection(segs)
        ax.add_collection(lc)

        # Show plot
        plt.title(r'${} \times {}$ Graphene Sheet, $l={}$'.format(self.hex_n,self.hex_m,self.bond_length))
        plt.xlabel(r'$x\,\, (\AA)$')
        plt.ylabel(r'$y\,\, (\AA)$')
        plt.axis('equal')
        plt.show()

    # Plot atoms and bonds
    def plot_bonds(self):
        ax = plt.gca()
        plt.plot(self.x_coords,self.y_coords,'or')

        # Plot bonds
        segs = []
        for bond_num,bond in enumerate(self.bond_list):
            segs.append([
                (self.x_coords[bond[0]],self.y_coords[bond[0]]),
                (self.x_coords[bond[1]],self.y_coords[bond[1]])])
            # Calculate center of bond
            x_bar = (self.x_coords[bond[0]] + self.x_coords[bond[1]])/2
            y_bar = (self.y_coords[bond[0]] + self.y_coords[bond[1]])/2
            # Annotate
            plt.annotate('{:4d}'.format(bond_num),xy=(x_bar,y_bar))
        lc = LineCollection(segs)
        ax.add_collection(lc)

        # Show plot
        plt.title(r'${} \times {}$ Graphene Sheet, $l={}$'.format(self.hex_n,self.hex_m,self.bond_length))
        plt.xlabel(r'$x\,\, (\AA)$')
        plt.ylabel(r'$y\,\, (\AA)$')
        plt.axis('equal')
        plt.show()

