#!/usr/bin/python

import os.path
import pyurdme
import dolfin
import numpy
import scipy
import sys
import json

class Nucleus(dolfin.SubDomain):
    def inside(self,x,on_boundary):
        return dolfin.between(x[0]**2+x[1]**2+x[2]**2,(0,3.**2))


class hes1(pyurdme.URDMEModel):
    def __init__(self,model_name="hes1", P_start=200,m_start=10,k1_e=1.e9, k2_e=0.1, alpha_m_e=3.0, alpha_m_gamma_e=3./30., alpha_p_e=1.0, mu_m_e=0.015,mu_p_e=0.043):
        pyurdme.URDMEModel.__init__(self, model_name)
        self.P_start = P_start
        self.m_start = m_start

        #Species
        Pf = pyurdme.Species(name="Pf",diffusion_constant=0.,dimension=3)
        Po = pyurdme.Species(name="Po",diffusion_constant=0.,dimension=3)
        mRNA = pyurdme.Species(name="mRNA",diffusion_constant=6.e-1,dimension=3)
        protein = pyurdme.Species(name="protein",diffusion_constant=6.e-1,dimension=3)
        self.add_species([Pf,Po,mRNA,protein])

        #mesh in xml
        self.mesh = pyurdme.URDMEMesh.read_mesh("mesh/cell.xml")
        #Domains markers
        nucleus = [2]
        cytoplasm = [1]
        promoter_site = [3]
        #Domains
        self.add_subdomain(Nucleus(), nucleus[0])
        self.get_subdomain_vector()
        self.sd[self.mesh.closest_vertex([0,0,0])] = promoter_site[0]
        self.subdomains={}

        #Parameters
        k1 = pyurdme.Parameter(name="k1",expression=k1_e)
        k2 = pyurdme.Parameter(name="k2",expression=k2_e)
        alpha_m = pyurdme.Parameter(name="alpha_m",expression=alpha_m_e)
        alpha_m_gamma = pyurdme.Parameter(name="alpha_m_gamma",expression=alpha_m_gamma_e)
        alpha_p = pyurdme.Parameter(name="alpha_p",expression=alpha_p_e)
        mu_m = pyurdme.Parameter(name="mu_m",expression=mu_m_e)
        mu_p = pyurdme.Parameter(name="mu_p",expression=mu_p_e)
        self.add_parameter([k1,k2,alpha_m,alpha_m_gamma,alpha_p,mu_m,mu_p])


        #Reactions
        R1 = pyurdme.Reaction(name="R1",reactants={Pf:1,protein:1},products={Po:1},massaction=True,rate=k1, restrict_to=promoter_site)
        R2 = pyurdme.Reaction(name="R2",reactants={Po:1},products={Pf:1,protein:1},massaction=True,rate=k2, restrict_to=promoter_site)
        R3 = pyurdme.Reaction(name="R3",reactants={Pf:1},products={Pf:1,mRNA:1},massaction=True,rate=alpha_m, restrict_to=promoter_site)
        R4 = pyurdme.Reaction(name="R4",reactants={Po:1},products={Po:1,mRNA:1},massaction=True,rate=alpha_m_gamma, restrict_to=promoter_site)
        R5 = pyurdme.Reaction(name="R5",reactants={mRNA:1},products={mRNA:1,protein:1},massaction=True,rate=alpha_p,restrict_to=cytoplasm)
        R6 = pyurdme.Reaction(name="R6",reactants={mRNA:1},products={},massaction=True,rate=mu_m)
        R7 = pyurdme.Reaction(name="R7",reactants={protein:1},products={},massaction=True,rate=mu_p)
        self.add_reaction([R1,R2,R3,R4,R5,R6,R7])

        #Restrict to promoter_site
        self.restrict(Po,promoter_site)
        self.restrict(Pf,promoter_site)

        #Distribute molecules over the mesh
        self.set_initial_condition_scatter({Pf:1},promoter_site)
        self.set_initial_condition_scatter({protein:P_start},cytoplasm)
        self.set_initial_condition_scatter({mRNA:m_start},nucleus)

        self.timespan(range(400))

def g2(result):
    #import features as f
    import time as t
    import numpy as np
    import scipy
    
    def burstiness(y):
        """
        % DN_Burstiness
        % 
        % Returns the 'burstiness' statistic from:
        % 
        % Goh and Barabasi, 'Burstiness and memory in complex systems' Europhys. Lett.
        % 81, 48002 (2008)
        % 
        % INPUTS:
        % y, the input time series """
    
        return (np.std(y) - np.mean(y))/(np.std(y) + np.mean(y))

    def skewness(y):
        """
        % Estimates custom skewness measures, the Pearson skewnesses.
        % 
        % INPUTS:
        % y, the input time series """

        return (3*np.mean(y)-np.median(y))/np.std(y)

    def CV(x, k=1):
        """
        % Calculates the coefficient of variation, sigma^k / mu^k, of order k
        % 
        % INPUTS:
        % 
        % x, the input time series
        % 
        % k, the order of coefficient of variation (k = 1 is usual) """

        return (np.std(x))**k / (np.mean(x))**k

    def autocorrelations(y ,k=10):
        """
        % Computes the autocorrelation of an input time series, y, at a time-lag up to k 
        % 
        % INPUTS:
        % y, a scalar time series column vector
        %       
        % Output is the autocorrelation for every time lag as an array """

        N = len(y)
        return [np.sum((y[0:N-i] - np.mean(y[0:N-i])) * (y[i:N] - np.mean(y[i:N]))/N/np.std(y[0:N-i])/np.std(y[i:N])) for i in range(1,k)]


    def check_if_zero(v):
        if np.count_nonzero(v) == 0:
            return True
        else: return False

    def check_arrays(data):
        a_len = data.shape[1]
        t_data = []
        for i,a in enumerate(data):
            if np.count_nonzero(a) == 0:
                idx = np.random.randint(0,a_len)
                a[idx] = 1
                data[i] = a
        return data

    def fft_norm(x):
        #remove dc bias
        x -= np.mean(x)
        return np.max(abs(np.fft.fft(x)))
    
    t0 = t.time()
    
    mapped ={}
    
    parameters = result.model.get_all_parameters()
    mapped['parameters'] = zip(parameters.keys(),(v.expression for v in parameters.values()))
    mapped['D'] = result.model.get_species('mRNA').diffusion_constant
    #mapped['tspan'] = result.model.tspan
    
    # Data converter
    result_species = []
    for species in result.model.get_all_species():
        if species == 'protein' or species == 'mRNA':
            #get result
            matrix = np.random.rand(400, 2474)
            # matrix = result.get_species(species)

            #Check for zero vectors and replace one random element to 1
            matrix = check_arrays(matrix)

            #Transpose the result, should make this matrix.T
            matrixT = np.asarray([list(matrix[:,i]) for i in range(matrix.shape[1])])

            matrixT = check_arrays(matrixT)

            result_species.append((matrix, matrixT))
    
    #Sums all CP in each snapshot(m) and per voxel(mT)
    total_sum = []
    for m, mT in result_species:
        m_sum = [np.sum(v) for v in m]
        mt_sum = [np.sum(v) for v in mT]
        #total_sum.append((scipy.signal.savgol_filter(m_sum, 51, 3), scipy.signal.savgol_filter(m_sum, 51, 3)))
        total_sum.append((m_sum, mt_sum))
    #feature vector
    f_vector = []
    
    #Total CP
    for c, (Sm, SmT) in enumerate(total_sum):
        f_vector.append(np.mean(Sm))
        f_vector.append(burstiness(Sm))
        f_vector.append(burstiness(SmT))
        f_vector.append(skewness(Sm))
        #f_vector.append(skewness(SmT)) remove
        f_vector.append(CV(Sm, 1))
        f_vector.append(CV(SmT, 1))
        f_vector.append(np.linalg.norm(autocorrelations(Sm,len(Sm)/2)))
        #f_vector.append(np.linalg.norm(autocorrelations(SmT,len(SmT)/2))) remove
        f_vector.append(fft_norm(Sm))
        f_vector.append(fft_norm(SmT))
        if c < len(total_sum)-1:                                         ### CHECK IF IT'S CORRECT! 03/18
            for i in range(1, len(total_sum)-c):
                #Correlations of total CP in volume
                f_vector.append(np.corrcoef(Sm,total_sum[c+i][0])[0][1])
                #f_vector.append(np.corrcoef(SmT,total_sum[c+i][1])[0][1]) remove
            
    
    for c, (m, mT) in enumerate(result_species):
        bm, bmT, sm, smT, CVm, CVmT, ACm, ACmT = [],[],[],[],[],[],[],[]
        for v in m:
            bm.append(burstiness(v))
            sm.append(skewness(v))
            CVm.append(CV(v, 1))
            #ACm.append(numpy.linalg.norm(f.autocorrelations(v, len(v)/2)))
        for v in mT:
            bmT.append(burstiness(v))
            smT.append(skewness(v))
            CVmT.append(CV(v,1))
            #ACmT.append(numpy.linalg.norm(f.autocorrelations(v, len(v)/2)))
        if c < len(result_species):
            for i in range(1, len(result_species)-c):
                nm, nmT = result_species[c+i]
                corr1 = [np.corrcoef(v, nm[e])[0][1] for e, v in enumerate(m)]
                corr2 = [np.corrcoef(v, nmT[e])[0][1] for e, v in enumerate(mT)]
                f_vector.append(np.linalg.norm(corr1))
                f_vector.append(np.linalg.norm(corr2))
                f_vector.append(np.var(corr1))
                f_vector.append(np.var(corr2))
        
        for var in [bm, bmT, sm, smT, CVm, CVmT]:
            f_vector.append(np.linalg.norm(var))
            f_vector.append(np.mean(var))
            f_vector.append(np.var(var))
            
    
    mapped['features'] = f_vector
    
    mapped['time for mapper (s)'] = t.time() - t0
    
    return mapped

if __name__ == "__main__":

    # assume that the first and the second arguments are k1 and k2

    if len(sys.argv) == 3:
        k1_e = float(sys.argv[1])
        k2_e = float(sys.argv[2])
    else:
        k1_e = 1e9
        k2_e = 0.1
        
    os.chdir("/hes1simulation")
    model = hes1(model_name="hes1",k1_e=k1_e,k2_e=k2_e)
    result = model.run(report_level=0)
    mapped = g2(result)
    print ("result:%s" % json.dumps(mapped))
