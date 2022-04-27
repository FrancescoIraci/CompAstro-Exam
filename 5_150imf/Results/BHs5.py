import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import numpy as np

nocsi=dd.read_csv("nocsi_output/output*.csv")
o2csi=dd.read_csv("02_output/output*.csv")
o4csi=dd.read_csv("04_output/output*.csv")
r_dt=dd.read_csv("r_sevn_output/output*.csv")
d_dt=dd.read_csv("d_sevn_output/output*.csv")

#Consider only the final states
nocsi=nocsi.drop_duplicates(["ID","name"], keep='last')
o2csi=o2csi.drop_duplicates(["ID","name"], keep='last')
o4csi=o4csi.drop_duplicates(["ID","name"], keep='last')
r_dt=r_dt.drop_duplicates(["ID","name"], keep='last')
d_dt=d_dt.drop_duplicates(["ID","name"], keep='last')

#Load evolved file
r_dte=dd.read_csv("r_sevn_output/evolved_*.dat",sep='\s+')
#Give a look to the columns
print(r_dte.columns)
r_dte=r_dte.rename(columns={'#ID': 'ID','Mass_0':"Mzams_0", 'Mass_1':"Mzams_1"})
#After change
print(r_dte.columns)
#Join the two dataset
r_dt = r_dt.merge(r_dte, on=["ID","name"],  how="inner", suffixes=("","_ini") )

d_dte = dd.read_csv("d_sevn_output/evolved_*.dat", sep='\s+')
# Give a look to the columns
print(d_dte.columns)
d_dte = d_dte.rename(columns={'#ID': 'ID', 'Mass_0':"Mzams_0", 'Mass_1':"Mzams_1"})
#After change
print(d_dte.columns)
#Join the two dataset
d_dt = d_dt.merge(d_dte, on=["ID","name"],  how="inner", suffixes=("","_ini") )

nocsi_dte=dd.read_csv("nocsi_output/evolved_*.dat",sep='\s+')
#Give a look to the columns
print(nocsi_dte.columns)
nocsi_dte=nocsi_dte.rename(columns={'#ID': 'ID','Mass_0':"Mzams_0", 'Mass_1':"Mzams_1"})
#After change
print(nocsi_dte.columns)
#Join the two dataset
nocsi = nocsi.merge(nocsi_dte, on=["ID","name"],  how="inner", suffixes=("","_ini") )

o2_dte=dd.read_csv("nocsi_output/evolved_*.dat",sep='\s+')
#Give a look to the columns
print(o2_dte.columns)
o2_dte=o2_dte.rename(columns={'#ID': 'ID','Mass_0':"Mzams_0", 'Mass_1':"Mzams_1"})
#After change
print(o2_dte.columns)
#Join the two dataset
o2csi = o2csi.merge(o2_dte, on=["ID","name"],  how="inner", suffixes=("","_ini") )

o4_dte=dd.read_csv("nocsi_output/evolved_*.dat",sep='\s+')
#Give a look to the columns
print(o4_dte.columns)
o4_dte=o4_dte.rename(columns={'#ID': 'ID','Mass_0':"Mzams_0", 'Mass_1':"Mzams_1"})
#After change
print(o4_dte.columns)
#Join the two dataset
o4csi = o4csi.merge(o4_dte, on=["ID","name"],  how="inner", suffixes=("","_ini") )
#---------------------------------------- Rapid --------------------------------------

#Create filter indexes 
r_idx0 = (r_dt.RemnantType_0==6) & (r_dt.RemnantType_1!=0) & (r_dt.RemnantType_1!=-1)
r_idx1 = (r_dt.RemnantType_1==6) & (r_dt.RemnantType_0!=0) & (r_dt.RemnantType_0!=-1)
r_idxb0 = r_idx0  & r_dt.Semimajor.notnull()
r_idxb1 = r_idx1  & r_dt.Semimajor.notnull()
r_idxm0 = r_idxb0 & (r_dt.GWtime + r_dt.BWorldtime  <= 14000)
r_idxm1 = r_idxb1 & (r_dt.GWtime + r_dt.BWorldtime  <= 14000)

#Filter and join masses in new dataframe
r_AllBH = dd.concat([r_dt[r_idx0].Mass_0,r_dt[r_idx1].Mass_1])
r_BoundBH = dd.concat([r_dt[r_idxb0].Mass_0,r_dt[r_idxb1].Mass_1])
r_MergingBH = dd.concat([r_dt[r_idxm0].Mass_0,r_dt[r_idxm1].Mass_1])

r_dfAllBH= r_AllBH.compute()
r_dfBound= r_BoundBH.compute()
r_dfMerg = r_MergingBH.compute()

#Filter and join initial masses
r_AllBHzams = dd.concat([r_dt[r_idx0].Mzams_0,r_dt[r_idx1].Mzams_1])
r_BoundBHzams = dd.concat([r_dt[r_idxb0].Mzams_0,r_dt[r_idxb1].Mzams_1])
r_MergingBHzams = dd.concat([r_dt[r_idxm0].Mzams_0,r_dt[r_idxm1].Mzams_1])

r_AllBHzams=r_AllBHzams.compute()
r_BoundBHzams=r_BoundBHzams.compute()
r_MergingBHzams=r_MergingBHzams.compute()

#---------------------------------------- Delayed --------------------------------------

#Create filter indexes NOCSI
d_idx0 = (d_dt.RemnantType_0==6) & (d_dt.RemnantType_1!=0) & (d_dt.RemnantType_1!=-1)
d_idx1 = (d_dt.RemnantType_1==6) & (d_dt.RemnantType_0!=0) & (d_dt.RemnantType_0!=-1)
d_idxb0 = d_idx0  & d_dt.Semimajor.notnull()
d_idxb1 = d_idx1  & d_dt.Semimajor.notnull()
d_idxm0 = d_idxb0 & (d_dt.GWtime + d_dt.BWorldtime  <= 14000)
d_idxm1 = d_idxb1 & (d_dt.GWtime + d_dt.BWorldtime  <= 14000)

#Filter and join masses in new dataframe NOCSI
d_AllBH = dd.concat([d_dt[d_idx0].Mass_0,d_dt[d_idx1].Mass_1])
d_BoundBH = dd.concat([d_dt[d_idxb0].Mass_0,d_dt[d_idxb1].Mass_1])
d_MergingBH = dd.concat([d_dt[d_idxm0].Mass_0,d_dt[d_idxm1].Mass_1])

d_dfAllBH= d_AllBH.compute()
d_dfBound= d_BoundBH.compute()
d_dfMerg = d_MergingBH.compute()

#Filter and join initial masses
d_AllBHzams = dd.concat([d_dt[d_idx0].Mzams_0,d_dt[d_idx1].Mzams_1])
d_BoundBHzams = dd.concat([d_dt[d_idxb0].Mzams_0,d_dt[d_idxb1].Mzams_1])
d_MergingBHzams = dd.concat([d_dt[d_idxm0].Mzams_0,d_dt[d_idxm1].Mzams_1])

d_AllBHzams=d_AllBHzams.compute()
d_BoundBHzams=d_BoundBHzams.compute()
d_MergingBHzams=d_MergingBHzams.compute()

#---------------------------------------- SNC25TS = -1 --------------------------------------

#Create filter indexes NOCSI
idx0 = (nocsi.RemnantType_0==6) & (nocsi.RemnantType_1!=0) & (nocsi.RemnantType_1!=-1)
idx1 = (nocsi.RemnantType_1==6) & (nocsi.RemnantType_0!=0) & (nocsi.RemnantType_0!=-1)
idxb0 = idx0  & nocsi.Semimajor.notnull()
idxb1 = idx1  & nocsi.Semimajor.notnull()
idxm0 = idxb0 & (nocsi.GWtime + nocsi.BWorldtime  <= 14000)
idxm1 = idxb1 & (nocsi.GWtime + nocsi.BWorldtime  <= 14000)

#Filter and join masses in new dataframe NOCSI
AllBH = dd.concat([nocsi[idx0].Mass_0,nocsi[idx1].Mass_1])
BoundBH = dd.concat([nocsi[idxb0].Mass_0,nocsi[idxb1].Mass_1])
MergingBH = dd.concat([nocsi[idxm0].Mass_0,nocsi[idxm1].Mass_1])

dfAllBH= AllBH.compute()
dfBound= BoundBH.compute()
dfMerg = MergingBH.compute()

#-----Discard those wrong BHS that sevn generate for error-------
dfAllBH=pd.DataFrame(dfAllBH)
dfAllBH.columns=['Mass']
dfBound=pd.DataFrame(dfBound)
dfBound.columns=['Mass']
dfMerg=pd.DataFrame(dfMerg)
dfMerg.columns=['Mass']

indexNames = dfAllBH[ (dfAllBH['Mass'] < 3.15) ].index
dfAllBH.drop(indexNames,inplace = True)
#print(len(dfAllBH))
dfAllBH = dd.from_pandas(dfAllBH,npartitions=1)
dfAllBH=dfAllBH.compute()

indexNamesB = dfBound[ (dfBound['Mass'] < 3.15)].index
dfBound.drop(indexNamesB,inplace = True)
#print(len(BoundBH))
dfBound = dd.from_pandas(dfBound,npartitions=1)
dfBound=dfBound.compute()

indexNamesM = dfMerg[ (dfMerg['Mass'] < 3.15) ].index
dfMerg.drop(indexNamesM,inplace = True)
#print(len(MergingBH))
dfMerg = dd.from_pandas(dfMerg,npartitions=1)
dfMerg=dfMerg.compute()
# -------------------------------------------------------------
# -------------------------------------------------------------

#Filter and join initial masses
nocsi_AllBHzams = dd.concat([nocsi[idx0].Mzams_0,nocsi[idx1].Mzams_1])
nocsi_BoundBHzams = dd.concat([nocsi[idxb0].Mzams_0,nocsi[idxb1].Mzams_1])
nocsi_MergingBHzams = dd.concat([nocsi[idxm0].Mzams_0,nocsi[idxm1].Mzams_1])

nocsi_AllBHzams=nocsi_AllBHzams.compute()
nocsi_BoundBHzams=nocsi_BoundBHzams.compute()
nocsi_MergingBHzams=nocsi_MergingBHzams.compute()

#---------------------------------------- CSI = 0.2 --------------------------------------

#Create filter indexes CSI=02
o2_idx0 = (o2csi.RemnantType_0==6) & (o2csi.RemnantType_1!=0) & (o2csi.RemnantType_1!=-1)
o2_idx1 = (o2csi.RemnantType_1==6) & (o2csi.RemnantType_0!=0) & (o2csi.RemnantType_0!=-1)
o2_idxb0 = o2_idx0  & o2csi.Semimajor.notnull()
o2_idxb1 = o2_idx1  & o2csi.Semimajor.notnull()
o2_idxm0 = o2_idxb0 & (o2csi.GWtime + o2csi.BWorldtime  <= 14000)
o2_idxm1 = o2_idxb1 & (o2csi.GWtime + o2csi.BWorldtime  <= 14000)

#Filter and join masses in new dataframe CSI=02
o2_AllBH = dd.concat([o2csi[o2_idx0].Mass_0,o2csi[o2_idx1].Mass_1])
o2_BoundBH = dd.concat([o2csi[o2_idxb0].Mass_0,o2csi[o2_idxb1].Mass_1])
o2_MergingBH = dd.concat([o2csi[o2_idxm0].Mass_0,o2csi[o2_idxm1].Mass_1])

o2_dfAllBH= o2_AllBH.compute()
o2_dfBound= o2_BoundBH.compute()
o2_dfMerg = o2_MergingBH.compute()

#-----Discard those wrong BHS that sevn generate for error-------
o2_dfAllBH=pd.DataFrame(o2_dfAllBH)
o2_dfAllBH.columns=['Mass']
o2_dfBound=pd.DataFrame(o2_dfBound)
o2_dfBound.columns=['Mass']
o2_dfMerg=pd.DataFrame(o2_dfMerg)
o2_dfMerg.columns=['Mass']

o2_indexNames = o2_dfAllBH[ (o2_dfAllBH['Mass'] < 3.15) ].index
o2_dfAllBH.drop(o2_indexNames,inplace = True)
#print(len(dfAllBH))
o2_dfAllBH = dd.from_pandas(o2_dfAllBH,npartitions=1)
o2_dfAllBH=o2_dfAllBH.compute()

o2_indexNamesB = o2_dfBound[ (o2_dfBound['Mass'] < 3.15)].index
o2_dfBound.drop(o2_indexNamesB,inplace = True)
#print(len(BoundBH))
o2_dfBound = dd.from_pandas(o2_dfBound,npartitions=1)
o2_dfBound=o2_dfBound.compute()

o2_indexNamesM = o2_dfMerg[ (o2_dfMerg['Mass'] < 3.15) ].index
o2_dfMerg.drop(o2_indexNamesM,inplace = True)
#print(len(MergingBH))
o2_dfMerg = dd.from_pandas(o2_dfMerg,npartitions=1)
o2_dfMerg=o2_dfMerg.compute()
# -------------------------------------------------------------
# -------------------------------------------------------------

#Filter and join initial masses
o2_AllBHzams = dd.concat([o2csi[o2_idx0].Mzams_0,o2csi[o2_idx1].Mzams_1])
o2_BoundBHzams = dd.concat([o2csi[o2_idxb0].Mzams_0,o2csi[o2_idxb1].Mzams_1])
o2_MergingBHzams = dd.concat([o2csi[o2_idxm0].Mzams_0,o2csi[o2_idxm1].Mzams_1])

o2_AllBHzams=o2_AllBHzams.compute()
o2_BoundBHzams=o2_BoundBHzams.compute()
o2_MergingBHzams=o2_MergingBHzams.compute()

# ---------------------------------------- CSI = 0.4 --------------------------------------

#Create filter indexes CSI=04
o4_idx0 = (o4csi.RemnantType_0==6) & (o4csi.RemnantType_1!=0) & (o4csi.RemnantType_1!=-1)
o4_idx1 = (o4csi.RemnantType_1==6) & (o4csi.RemnantType_0!=0) & (o4csi.RemnantType_0!=-1)
o4_idxb0 = o4_idx0  & o4csi.Semimajor.notnull()
o4_idxb1 = o4_idx1  & o4csi.Semimajor.notnull()
o4_idxm0 = o4_idxb0 & (o4csi.GWtime + o4csi.BWorldtime  <= 14000)
o4_idxm1 = o4_idxb1 & (o4csi.GWtime + o4csi.BWorldtime  <= 14000)

#Filter and join masses in new dataframe CSI=04
o4_AllBH = dd.concat([o4csi[o4_idx0].Mass_0,o4csi[o4_idx1].Mass_1])
o4_BoundBH = dd.concat([o4csi[o4_idxb0].Mass_0,o4csi[o4_idxb1].Mass_1])
o4_MergingBH = dd.concat([o4csi[o4_idxm0].Mass_0,o4csi[o4_idxm1].Mass_1])

o4_dfAllBH= o4_AllBH.compute()
o4_dfBound= o4_BoundBH.compute()
o4_dfMerg = o4_MergingBH.compute()

#-----Discard those wrong BHS that sevn generates for error-------
o4_dfAllBH=pd.DataFrame(o4_dfAllBH)
o4_dfAllBH.columns=['Mass']
o4_dfBound=pd.DataFrame(o4_dfBound)
o4_dfBound.columns=['Mass']
o4_dfMerg=pd.DataFrame(o4_dfMerg)
o4_dfMerg.columns=['Mass']

o4_indexNames = o4_dfAllBH[ (o4_dfAllBH['Mass'] < 3.15) ].index
o4_dfAllBH.drop(o4_indexNames,inplace = True)
#print(len(dfAllBH))
o4_dfAllBH = dd.from_pandas(o4_dfAllBH,npartitions=1)
o4_dfAllBH=o4_dfAllBH.compute()

o4_indexNamesB = o4_dfBound[ (o4_dfBound['Mass'] < 3.15)].index
o4_dfBound.drop(o4_indexNamesB,inplace = True)
#print(len(BoundBH))
o4_dfBound = dd.from_pandas(o4_dfBound,npartitions=1)
o4_dfBound=o4_dfBound.compute()

o4_indexNamesM = o4_dfMerg[ (o4_dfMerg['Mass'] < 3.15) ].index
o4_dfMerg.drop(o4_indexNamesM,inplace = True)
#print(len(MergingBH))
o4_dfMerg = dd.from_pandas(o4_dfMerg,npartitions=1)
o4_dfMerg=o4_dfMerg.compute()
# -------------------------------------------------------------
# -------------------------------------------------------------

#Filter and join initial masses
o4_AllBHzams = dd.concat([o4csi[o4_idx0].Mzams_0,o4csi[o4_idx1].Mzams_1])
o4_BoundBHzams = dd.concat([o4csi[o4_idxb0].Mzams_0,o4csi[o4_idxb1].Mzams_1])
o4_MergingBHzams = dd.concat([o4csi[o4_idxm0].Mzams_0,o4csi[o4_idxm1].Mzams_1])

o4_AllBHzams=o4_AllBHzams.compute()
o4_BoundBHzams=o4_BoundBHzams.compute()
o4_MergingBHzams=o4_MergingBHzams.compute()

#------------------------------------- PLOT -----------------------------

plt.figure(figsize=(16,10))

#Binning

minM = dfAllBH.Mass.min()
maxM = dfAllBH.Mass.max()
mybins = np.arange(minM,maxM, 2)

# Histograms

# ------------------------------------------- BH spectrum ------------------------------------------------
#-------------------------------------------- Bounded ----------------------------------------------------
plt.subplot(2,3,1)
plt.hist(dfBound,bins=mybins,histtype="step",lw=2,label=r'Compactness' + ' Patton(2020)', color='blue')
# plt.hist(o2_dfBound,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.2$',color='dodgerblue')
# plt.hist(o4_dfBound,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.4$'color='darkturquoise')
plt.hist(r_dfBound,bins=mybins,histtype="step",lw=2,label="Rapid", color = 'red')
plt.hist(d_dfBound,bins=mybins,histtype="step",lw=2,label="Delayed", color = 'darkgreen')
plt.yscale("log")
plt.legend(title='Bounded BHs')
#plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
plt.ylabel("Number",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)

plt.subplot(2,3,2)
#plt.hist(dfBound,bins=mybins,histtype="step",lw=2,label=r'Compactness' + ' Patton(2020)', color='blue')
plt.hist(o2_dfBound,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.2$',color='dodgerblue')
# plt.hist(o4_dfBound,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.4$'color='darkturquoise')
plt.hist(r_dfBound,bins=mybins,histtype="step",lw=2,label="Rapid", color = 'red')
plt.hist(d_dfBound,bins=mybins,histtype="step",lw=2,label="Delayed", color = 'darkgreen')
plt.yscale("log")
plt.legend(title='Bounded BHs')
#plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
#plt.ylabel("$N$",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)

plt.subplot(2,3,3)
#plt.hist(dfBound,bins=mybins,histtype="step",lw=2,label=r'Compactness' + ' Patton(2020)', color='blue')
#plt.hist(o2_dfBound,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.2$',color='dodgerblue')
plt.hist(o4_dfBound,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.4$',color='darkturquoise')
plt.hist(r_dfBound,bins=mybins,histtype="step",lw=2,label="Rapid", color = 'red')
plt.hist(d_dfBound,bins=mybins,histtype="step",lw=2,label="Delayed", color = 'darkgreen')
plt.yscale("log")
plt.legend(title='Bounded BHs')
#plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
#plt.ylabel("$N$",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)

#-------------------------------------------- Merging ----------------------------------------------------
plt.subplot(2,3,4)
plt.hist(dfMerg,bins=mybins,histtype="step",lw=2,label=r"Compactness" + ' Patton(2020)',color='blue')
# plt.hist(o2_dfMerg,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.2$',color='dodgerblue')
# plt.hist(o4_dfMerg,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.4$'color='darkturquoise')
plt.hist(r_dfMerg,bins=mybins,histtype="step",lw=2,label="Rapid",color= 'red')
plt.hist(d_dfMerg,bins=mybins,histtype="step",lw=2,label="Delayed", color = 'darkgreen')
plt.yscale("log")
plt.xlim(0,25)
plt.legend(title='Merging BHs')
plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
plt.ylabel("Number",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)

plt.subplot(2,3,5)
#plt.hist(dfMerg,bins=mybins,histtype="step",lw=2,label=r'Compactness' + ' Patton(2020)', color='blue')
plt.hist(o2_dfMerg,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.2$',color='dodgerblue')
# plt.hist(o4_dfMerg,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.4$'color='darkturquoise')
plt.hist(r_dfMerg,bins=mybins,histtype="step",lw=2,label="Rapid", color = 'red')
plt.hist(d_dfMerg,bins=mybins,histtype="step",lw=2,label="Delayed", color = 'darkgreen')
plt.yscale("log")
plt.xlim(0,25)
plt.legend(title='Merging BHs')
plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
#plt.ylabel("$N$",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)

plt.subplot(2,3,6)
#plt.hist(dfMerg,bins=mybins,histtype="step",lw=2,label=r'Compactness' + ' Patton(2020)', color='blue')
#plt.hist(o2_dfMerg,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.2$',color='dodgerblue')
plt.hist(o4_dfMerg,bins=mybins,histtype='step',lw=2,label=r'Compactness $\xi=0.4$',color='darkturquoise')
plt.hist(r_dfMerg,bins=mybins,histtype="step",lw=2,label="Rapid", color = 'red')
plt.hist(d_dfMerg,bins=mybins,histtype="step",lw=2,label="Delayed", color = 'darkgreen')
plt.yscale("log")
plt.xlim(0,25)
plt.legend(title='Merging BHs')
plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
#plt.ylabel("$N$",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)


#plt.tight_layout()
#plt.savefig("RDspectrum.pdf")
plt.show()





