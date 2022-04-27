import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#-------------------------------- BSE ----------------------------------------
#Load file
dt=dd.read_csv("c_sevn_output/output_*.csv")
#Give a look to the columns
print(dt.columns)
#Consider only the final states
dt=dt.drop_duplicates(["ID","name"], keep='last')

#Load evolved file
dte=dd.read_csv("c_sevn_output/evolved_*.dat",sep='\s+')
#Give a look to the columns
print(dte.columns)
dte=dte.rename(columns={'#ID': 'ID','Mass_0':"Mzams_0", 'Mass_1':"Mzams_1"})
#After change
print(dte.columns)

#Join the two dataset
dt = dt.merge(dte, on=["ID","name"],  how="inner", suffixes=("","_ini") )
# - on: column(s, can be a list of columns) to match during the merge of the two tables. The colum(s) has(have) to be present in both the tables
# - how: type of join to use, see documentation here and the next slide
# - suffixes: columns with the same name in the two tables (not used in on) will be renamed adding these suffixes.
#Give a look to the columns
print(dt.columns)


#Create filter indexes
idx0 = (dt.RemnantType_0==6) & (dt.RemnantType_1!=-1)
idx1 = (dt.RemnantType_1==6) & (dt.RemnantType_0!=-1)
idxb0 = idx0  & dt.Semimajor.notnull()
idxb1 = idx1  & dt.Semimajor.notnull()
idxm0 = idxb0 & (dt.GWtime + dt.BWorldtime  <= 14000)
idxm1 = idxb1 & (dt.GWtime + dt.BWorldtime  <= 14000)

#Filter and join masses
AllBH = dd.concat([dt[idx0].Mass_0,dt[idx1].Mass_1])
BoundBH = dd.concat([dt[idxb0].Mass_0,dt[idxb1].Mass_1])
MergingBH = dd.concat([dt[idxm0].Mass_0,dt[idxm1].Mass_1])

AllBH = AllBH.compute()
BoundBH = BoundBH.compute()
MergingBH= MergingBH.compute()

#-----With Compactness model there's an error in SEVN which classifies as BHs a few objects that in logfile
# ---- were classified as NS. They indeed have masses around 3 Msun --------------------------------------------
AllBH=pd.DataFrame(AllBH)
AllBH.columns=['Mass']
BoundBH=pd.DataFrame(BoundBH)
BoundBH.columns=['Mass']
MergingBH=pd.DataFrame(MergingBH)
MergingBH.columns=['Mass']

# ---- Find the index of those BHs and drop them ----------------
indexNames = AllBH[ (AllBH['Mass'] < 3.15) ].index
AllBH.drop(indexNames,inplace = True)
#print(len(AllBH))
AllBH = dd.from_pandas(AllBH,npartitions=1)
AllBH=AllBH.compute()

indexNamesB = BoundBH[ (BoundBH['Mass'] < 3.15)].index
BoundBH.drop(indexNamesB,inplace = True)
#print(len(BoundBH))
BoundBH = dd.from_pandas(BoundBH,npartitions=1)
BoundBH=BoundBH.compute()

indexNamesM = MergingBH[ (MergingBH['Mass'] < 3.15) ].index
MergingBH.drop(indexNamesM,inplace = True)
#print(len(MergingBH))
MergingBH = dd.from_pandas(MergingBH,npartitions=1)
MergingBH=MergingBH.compute()
# -------------------------------------------------------------
# -------------------------------------------------------------

#Filter and join initial masses
AllBHzams = dd.concat([dt[idx0].Mzams_0,dt[idx1].Mzams_1])
BoundBHzams = dd.concat([dt[idxb0].Mzams_0,dt[idxb1].Mzams_1])
MergingBHzams = dd.concat([dt[idxm0].Mzams_0,dt[idxm1].Mzams_1])

AllBHzams=AllBHzams.compute()
BoundBHzams=BoundBHzams.compute()
MergingBHzams=MergingBHzams.compute()

#-----With Compactness model there's an error which in SEVN which classifies as BHs a few objects that in logfile
# ---- were classified as NS. They indeed have masses around 3 Msun --------------------------------------------

AllBHzams=pd.DataFrame(AllBHzams)
BoundBHzams=pd.DataFrame(BoundBHzams)
MergingBHzams=pd.DataFrame(MergingBHzams)

AllBHzams.drop(indexNames,inplace = True)
AllBHzams = dd.from_pandas(AllBHzams,npartitions=1)
AllBHzams=AllBHzams.compute()

BoundBHzams.drop(indexNamesB,inplace = True)
BoundBHzams = dd.from_pandas(BoundBHzams,npartitions=1)
BoundBHzams=BoundBHzams.compute()

MergingBHzams.drop(indexNamesM,inplace = True)
MergingBHzams = dd.from_pandas(MergingBHzams,npartitions=1)
MergingBHzams=MergingBHzams.compute()
# -------------------------------------------------------------
# -------------------------------------------------------------

#-------------------------------------- SSE ------------------------------------

#Load file
Sdt=dd.read_csv("SSEc_sevn_output/output_*.csv")
#Give a look to the columns
print(Sdt.columns)
#Consider only the final states
Sdt=Sdt.drop_duplicates(["ID","name"], keep='last')

#Load evolved file
Sdte=dd.read_csv("SSEc_sevn_output/evolved_*.dat",sep='\s+')
#Give a look to the columns
print(Sdte.columns)
Sdte=Sdte.rename(columns={'#ID': 'ID','Mass':"Mzams"})
#After change
print(Sdte.columns)

#Join the two dataset
Sdt = Sdt.merge(Sdte, on=["ID","name"],  how="inner", suffixes=("","_ini") )
# - on: column(s, can be a list of columns) to match during the merge of the two tables. The colum(s) has(have) to be present in both the tables
# - how: type of join to use, see documentation here and the next slide
# - suffixes: columns with the same name in the two tables (not used in on) will be renamed adding these suffixes.
#Give a look to the columns
print(Sdt.compute())

#Create filter indexes
idx0 = (Sdt.RemnantType==6)

#Filter and join masses
S_AllBH = dd.concat([Sdt[idx0].Mass])

S_AllBH = S_AllBH.compute()

#Filter and join initial masses
S_AllBHzams = dd.concat([Sdt[idx0].Mzams])

S_AllBHzams=S_AllBHzams.compute()

#-------------------------------------------------------------------------------

#Plot
plt.figure(figsize=(10,5))

#binning
minM = AllBH.Mass.min()
maxM = AllBH.Mass.max()
print("Minimum mass",minM)
print("Maximum mass", maxM)
mybins = np.arange(minM,maxM, 3)
print("Minimum zams mass:",AllBHzams.min())
print("Maximum zams mass:",AllBHzams.max())

# BH vs ZAMS
plt.subplot(1,2,1)
plt.scatter(AllBHzams,AllBH,zorder=1,edgecolor="k",s=30,label="All")
plt.scatter(BoundBHzams,BoundBH,zorder=2,edgecolor="k",s=30, label="Bound")
plt.scatter(MergingBHzams,MergingBH,zorder=3,edgecolor="k",s=30, label="Merging")
plt.scatter(S_AllBHzams,S_AllBH,zorder=4,marker='d',s=2,label="SSE")
plt.plot(np.linspace(0,140),np.linspace(0,140),ls="dashed",c="gray")
plt.xscale("log")
plt.yscale("log")
plt.ylabel("BH mass [M$_\odot$]",fontsize=18)
plt.xlabel("$M\mathrm{zams}$  [M$_\odot$]",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)
plt.legend(fontsize=16)

plt.subplot(1,2,2)
# BH Spectrum
plt.hist(AllBH,bins=mybins,histtype="step",lw=3,label="All")
plt.hist(BoundBH,bins=mybins,histtype="step",lw=3,label="Bound")
plt.hist(MergingBH,bins=mybins,histtype="step",lw=3,label="Merging")
plt.yscale("log")
plt.legend(fontsize=15)
plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
plt.ylabel("$N$",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)


plt.tight_layout()
plt.savefig("zams_c.pdf")
plt.show()

#---------------------------------------------------------------
#formation rate

N=1e4

Nbh = len(AllBH)
Nbh_b = len(BoundBH)
Nbh_m = len(MergingBH)

f_tot = Nbh/N
f_b = Nbh_b/N
f_m = Nbh_m/N

print('Total number of Bhs produced:', Nbh,'\n','Number of bounded Bhs:', Nbh_b,'\n','Number of merging Bhs:',Nbh_m)

print(" fraction of BHs produced:",f_tot,"\n","fraction of BHs bounded",f_b,"\n","fraction of BHs which will merge:",f_m)

print(" fraction of BHs bounded in BH population:", Nbh_b/Nbh, "\n","fraction of BHs that merge in BH population:" , Nbh_m/Nbh)


#---------------------------------------------------------------

