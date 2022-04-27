import pandas as pd
import matplotlib.pyplot as plt
import dask.dataframe as dd
import numpy as np

def p(m):
	p=k*m**(-alfa)
	return p

def ssp(m):
	p=ss_k*m**(-alfa)
	return p

# --------------------------------- BSE --------------------------------------------------

df = dd.read_table("5_150Msun.txt",sep="\s+")
print(df.columns)

print(np.min(df.Mass2.compute()))

m1 = dd.concat([df.Mass1])
m1 = m1.compute()
Allmasses = dd.concat([df.Mass1,df.Mass2])
#Allmasses = dd.concat([df.Mass1])

Allmasses = Allmasses.compute()

minM = m1.min()
maxM = m1.max()

logm = np.log10(minM)
logM = np.log10(maxM)

# print(m1,m2)

# binning
mybins = np.logspace(np.min(Allmasses),logM)

alfa= 2.3
k = (1-alfa)/(maxM**(1-alfa)-minM**(1-alfa)) # normalization constant
N = len(Allmasses.to_numpy())
x=np.random.rand(N)
y=np.zeros(N,float)
y=(((1-alfa)*x)/k+minM**(1-alfa))**(1/(1-alfa))
p1=np.zeros(N,float)
p1=p(y)

# --------------------------------- SSE ---------------------------------------------------

ss_df = dd.read_table("SEVNInputSingleMDS_Z_xxx.txt",sep="\s+")
print(ss_df.columns)

print(np.min(ss_df.Mass1.compute()))

ss_Allmasses = dd.concat([ss_df.Mass1])
ss_Allmasses = ss_Allmasses.compute()

ss_minM = ss_Allmasses.min()
ss_maxM = ss_Allmasses.max()

ss_logm = np.log10(ss_minM)
ss_logM = np.log10(ss_maxM)

# print(m1,m2)

#binning
ss_mybins = np.logspace(ss_logm,ss_logM)

ss_k = (1-alfa)/(ss_maxM**(1-alfa)-ss_minM**(1-alfa))
ss_N = len(ss_Allmasses.to_numpy())
ss_x=np.random.rand(ss_N)
ss_y=np.zeros(ss_N,float)
ss_y=(((1-alfa)*ss_x)/ss_k+ss_minM**(1-alfa))**(1/(1-alfa))
ss_p1=np.zeros(ss_N,float)
ss_p1=ssp(ss_y)

# --------------------------------- PLOT ------------------------------------------------

plt.figure(figsize=(10,5))

plt.hist(Allmasses,bins=mybins,histtype="step",log='true',lw=2,density='True',label='BSE')
plt.hist(ss_Allmasses,bins=ss_mybins,histtype="step",log='true',lw=2,density='True',label='SSE')
plt.plot(y,p1,label=r'$m^{-2.35}$')
plt.plot(ss_y,ss_p1, label=r'$m^{-2.35}$')
plt.xscale('log')
plt.xlabel('Initial mass [M$_{\odot}$]',fontsize=18)
plt.ylabel('PDF',fontsize=18)
plt.ylim(0,1e0)
plt.legend(fontsize=16,loc = 'upper right')

plt.savefig("imfs.pdf")
plt.show()
