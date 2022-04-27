import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import numpy as np

#--------------------------- SSE ----------------------------------------------

# -------------------- Rapid ------------------
#Load file
r_Sdt=dd.read_csv("SSEr_sevn_output/output_*.csv")
#Give a look to the columns
print(r_Sdt.columns)
#Consider only the final states
r_Sdt=r_Sdt.drop_duplicates(["ID","name"], keep='last')

#Load evolved file
r_Sdte=dd.read_csv("SSEr_sevn_output/evolved_*.dat",sep='\s+')
r_Sdte=r_Sdte.rename(columns={'#ID': 'ID','Mass':"Mzams"})
#After change
print(r_Sdte.columns)
#Join the two dataset
r_Sdt = r_Sdt.merge(r_Sdte, on=["ID","name"],  how="inner", suffixes=("","_ini") )
#Create filter indexes
idx0 = (r_Sdt.RemnantType==6)
#Filter and join masses
r_S_AllBH = dd.concat([r_Sdt[idx0].Mass])
r_S_AllBH = r_S_AllBH.compute()
#Filter and join initial masses
r_S_AllBHzams = dd.concat([r_Sdt[idx0].Mzams])
r_S_AllBHzams=r_S_AllBHzams.compute()

# -------------------- Delayed -------------------
#Load file
d_Sdt=dd.read_csv("SSEd_sevn_output/output_*.csv")
#Give a look to the columns
print(d_Sdt.columns)
#Consider only the final states
d_Sdt=d_Sdt.drop_duplicates(["ID","name"], keep='last')

#Load evolved file
d_Sdte=dd.read_csv("SSEd_sevn_output/evolved_*.dat",sep='\s+')
d_Sdte=d_Sdte.rename(columns={'#ID': 'ID','Mass':"Mzams"})
#After change
print(d_Sdte.columns)
#Join the two dataset
d_Sdt = d_Sdt.merge(d_Sdte, on=["ID","name"],  how="inner", suffixes=("","_ini") )
#Create filter indexes
idx0 = (d_Sdt.RemnantType==6)
#Filter and join masses
d_S_AllBH = dd.concat([d_Sdt[idx0].Mass])
d_S_AllBH = d_S_AllBH.compute()
#Filter and join initial masses
d_S_AllBHzams = dd.concat([d_Sdt[idx0].Mzams])
d_S_AllBHzams=d_S_AllBHzams.compute()

# ------------------ SNC25 = -1 -------------------------------------------
#Load file
nocsi_Sdt=dd.read_csv("nocsi_SSEc_sevn_output/output_*.csv")
#Give a look to the columns
print(nocsi_Sdt.columns)
#Consider only the final states
nocsi_Sdt=nocsi_Sdt.drop_duplicates(["ID","name"], keep='last')

#Load evolved file
nocsi_Sdte=dd.read_csv("nocsi_SSEc_sevn_output/evolved_*.dat",sep='\s+')
nocsi_Sdte=nocsi_Sdte.rename(columns={'#ID': 'ID','Mass':"Mzams"})
#After change
print(nocsi_Sdte.columns)
#Join the two dataset
nocsi_Sdt = nocsi_Sdt.merge(nocsi_Sdte, on=["ID","name"],  how="inner", suffixes=("","_ini") )
#Create filter indexes
idx0 = (nocsi_Sdt.RemnantType==6)
#Filter and join masses
nocsi_S_AllBH = dd.concat([nocsi_Sdt[idx0].Mass])
nocsi_S_AllBH = nocsi_S_AllBH.compute()
#Filter and join initial masses
nocsi_S_AllBHzams = dd.concat([nocsi_Sdt[idx0].Mzams])
nocsi_S_AllBHzams=nocsi_S_AllBHzams.compute()

# ------------------ csi = 0.2 --------------------------------------------
#Load file
o2_Sdt=dd.read_csv("o2_SSEc_sevn_output/output_*.csv")
#Give a look to the columns
print(o2_Sdt.columns)
#Consider only the final states
o2_Sdt=o2_Sdt.drop_duplicates(["ID","name"], keep='last')

#Load evolved file
o2_Sdte=dd.read_csv("o2_SSEc_sevn_output/evolved_*.dat",sep='\s+')
o2_Sdte=o2_Sdte.rename(columns={'#ID': 'ID','Mass':"Mzams"})
#After change
print(o2_Sdte.columns)
#Join the two dataset
o2_Sdt = o2_Sdt.merge(o2_Sdte, on=["ID","name"],  how="inner", suffixes=("","_ini") )
#Create filter indexes
idx0 = (o2_Sdt.RemnantType==6)
#Filter and join masses
o2_S_AllBH = dd.concat([o2_Sdt[idx0].Mass])
o2_S_AllBH = o2_S_AllBH.compute()
#Filter and join initial masses
o2_S_AllBHzams = dd.concat([o2_Sdt[idx0].Mzams])
o2_S_AllBHzams=o2_S_AllBHzams.compute()

# ------------------- csi = 0.4 ---------------------------------------------
#Load file
o4_Sdt=dd.read_csv("o4_SSEc_sevn_output/output_*.csv")
#Give a look to the columns
print(o4_Sdt.columns)
#Consider only the final states
o4_Sdt=o4_Sdt.drop_duplicates(["ID","name"], keep='last')

#Load evolved file
o4_Sdte=dd.read_csv("o4_SSEc_sevn_output/evolved_*.dat",sep='\s+')
o4_Sdte=o4_Sdte.rename(columns={'#ID': 'ID','Mass':"Mzams"})
#After change
print(o4_Sdte.columns)
#Join the two dataset
o4_Sdt = o4_Sdt.merge(o4_Sdte, on=["ID","name"],  how="inner", suffixes=("","_ini") )
#Create filter indexes
idx0 = (o4_Sdt.RemnantType==6)
#Filter and join masses
o4_S_AllBH = dd.concat([o4_Sdt[idx0].Mass])
o4_S_AllBH = o4_S_AllBH.compute()
#Filter and join initial masses
o4_S_AllBHzams = dd.concat([o4_Sdt[idx0].Mzams])
o4_S_AllBHzams=o4_S_AllBHzams.compute()

# ------------------------ BSE ---------------------------------------------------------
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

o2_dte=dd.read_csv("02_output/evolved_*.dat",sep='\s+')
#Give a look to the columns
print(o2_dte.columns)
o2_dte=o2_dte.rename(columns={'#ID': 'ID','Mass_0':"Mzams_0", 'Mass_1':"Mzams_1"})
#After change
print(o2_dte.columns)
#Join the two dataset
o2csi = o2csi.merge(o2_dte, on=["ID","name"],  how="inner", suffixes=("","_ini") )

o4_dte=dd.read_csv("04_output/evolved_*.dat",sep='\s+')
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

nocsi_indexNames = dfAllBH[ (dfAllBH['Mass'] < 3.15) ].index
dfAllBH.drop(nocsi_indexNames,inplace = True)
#print(len(dfAllBH))
dfAllBH = dd.from_pandas(dfAllBH,npartitions=1)
dfAllBH=dfAllBH.compute()

nocsi_indexNamesB = dfBound[ (dfBound['Mass'] < 3.15)].index
dfBound.drop(nocsi_indexNamesB,inplace = True)
#print(len(BoundBH))
dfBound = dd.from_pandas(dfBound,npartitions=1)
dfBound=dfBound.compute()

nocsi_indexNamesM = dfMerg[ (dfMerg['Mass'] < 3.15) ].index
dfMerg.drop(nocsi_indexNamesM,inplace = True)
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

#-----Discard those wrong BHS that sevn generate for error-------
nocsi_AllBHzams=pd.DataFrame(nocsi_AllBHzams)
nocsi_BoundBHzams=pd.DataFrame(nocsi_BoundBHzams)
nocsi_MergingBHzams=pd.DataFrame(nocsi_MergingBHzams)

nocsi_AllBHzams.drop(nocsi_indexNames,inplace = True)
nocsi_AllBHzams = dd.from_pandas(nocsi_AllBHzams,npartitions=1)
nocsi_AllBHzams=nocsi_AllBHzams.compute()

nocsi_BoundBHzams.drop(nocsi_indexNamesB,inplace = True)
nocsi_BoundBHzams = dd.from_pandas(nocsi_BoundBHzams,npartitions=1)
nocsi_BoundBHzams=nocsi_BoundBHzams.compute()

nocsi_MergingBHzams.drop(nocsi_indexNamesM,inplace = True)
nocsi_MergingBHzams = dd.from_pandas(nocsi_MergingBHzams,npartitions=1)
nocsi_MergingBHzams=nocsi_MergingBHzams.compute()
# -------------------------------------------------------------
# -------------------------------------------------------------

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

#-----Discard those wrong BHS that sevn generate for error-------
o2_AllBHzams=pd.DataFrame(o2_AllBHzams)
o2_BoundBHzams=pd.DataFrame(o2_BoundBHzams)
o2_MergingBHzams=pd.DataFrame(o2_MergingBHzams)

o2_AllBHzams.drop(o2_indexNames,inplace = True)
o2_AllBHzams = dd.from_pandas(o2_AllBHzams,npartitions=1)
o2_AllBHzams=o2_AllBHzams.compute()

o2_BoundBHzams.drop(o2_indexNamesB,inplace = True)
o2_BoundBHzams = dd.from_pandas(o2_BoundBHzams,npartitions=1)
o2_BoundBHzams=o2_BoundBHzams.compute()

o2_MergingBHzams.drop(o2_indexNamesM,inplace = True)
o2_MergingBHzams = dd.from_pandas(o2_MergingBHzams,npartitions=1)
o2_MergingBHzams=o2_MergingBHzams.compute()
# -------------------------------------------------------------
# -------------------------------------------------------------

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

#-----Discard those wrong BHS that sevn generate for error-------
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

#-----Discard those wrong BHS that sevn generate for error-------
o4_AllBHzams=pd.DataFrame(o4_AllBHzams)
o4_BoundBHzams=pd.DataFrame(o4_BoundBHzams)
o4_MergingBHzams=pd.DataFrame(o4_MergingBHzams)

o4_AllBHzams.drop(o4_indexNames,inplace = True)
o4_AllBHzams = dd.from_pandas(o4_AllBHzams,npartitions=1)
o4_AllBHzams=o4_AllBHzams.compute()

o4_BoundBHzams.drop(o4_indexNamesB,inplace = True)
o4_BoundBHzams = dd.from_pandas(o4_BoundBHzams,npartitions=1)
o4_BoundBHzams=o4_BoundBHzams.compute()

o4_MergingBHzams.drop(o4_indexNamesM,inplace = True)
o4_MergingBHzams = dd.from_pandas(o4_MergingBHzams,npartitions=1)
o4_MergingBHzams=o4_MergingBHzams.compute()
# -------------------------------------------------------------
# -------------------------------------------------------------

#------------------------------------- PLOT -----------------------------

plt.figure(figsize=(15,18))

#Binning

minM = dfAllBH.Mass.min()
maxM = dfAllBH.Mass.max()
mybins = np.arange(minM,maxM, 2)

#Rapid
plt.subplot(5,2,1)
plt.hist(r_dfAllBH,bins=mybins,histtype="step",lw=2,label='All')
plt.hist(r_dfBound,bins=mybins,histtype='step',lw=2,label='Bounded')
plt.hist(r_dfMerg,bins=mybins,histtype='step',lw=2,label=r'Merging')
plt.gca().tick_params(axis='both', which='major', labelsize=18)
# plt.xscale('log')
plt.yscale('log')
plt.ylabel('Number',fontsize=18)
plt.legend(title='Rapid')

plt.subplot(5,2,2)
plt.scatter(r_AllBHzams,r_dfAllBH,zorder=1,edgecolor="k",s=30,label="All")
plt.scatter(r_BoundBHzams,r_dfBound,zorder=2,edgecolor="k",s=30, label="Bounded")
plt.scatter(r_MergingBHzams,r_dfMerg,zorder=3,edgecolor="k",s=30, label="Merging")
plt.scatter(r_S_AllBHzams,r_S_AllBH,zorder=4,marker='d',s=2,label='SSE')
plt.plot(np.linspace(0,140),np.linspace(0,140),ls="dashed",c="gray")
plt.yscale("log")
plt.xscale('log')
plt.legend(title='Rapid')
#plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
plt.ylabel(r'BH mass [M$_{\odot}$]',fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)

# #Delayed
plt.subplot(5,2,3)
plt.hist(d_dfAllBH,bins=mybins,histtype="step",lw=2,label='All')
plt.hist(d_dfBound,bins=mybins,histtype="step",lw=2,label="Bounded")
plt.hist(d_dfMerg,bins=mybins,histtype="step",lw=2,label="Merging")
plt.yscale("log")
plt.ylabel('Number',fontsize=18)
plt.legend(title='Delayed')
plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)

plt.subplot(5,2,4)
plt.scatter(d_AllBHzams,d_dfAllBH,zorder=1,edgecolor="k",s=30,label="All")
plt.scatter(d_BoundBHzams,d_dfBound,zorder=2,edgecolor="k",s=30, label="Bounded")
plt.scatter(d_MergingBHzams,d_dfMerg,zorder=3,edgecolor="k",s=30, label="Merging")
plt.scatter(d_S_AllBHzams,d_S_AllBH,zorder=4,marker='d',s=2,label='SSE')
plt.plot(np.linspace(0,140),np.linspace(0,140),ls="dashed",c="gray")
plt.yscale("log")
plt.xscale('log')
plt.ylabel('BH mass [$M_{\odot}$]',fontsize=18)
plt.xlabel(r'M$_{ZAMS}$'+'[M$_{\odot}$]',fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)
# plt.legend(title='Delayed')

# plt.savefig("RDspectrum.pdf")
# plt.show()

# Compactness no xi
plt.subplot(5,2,5)
plt.hist(dfAllBH,bins=mybins,histtype="step",lw=2,label='All')
plt.hist(dfBound,bins=mybins,histtype="step",lw=2,label="Bounded")
plt.hist(dfMerg,bins=mybins,histtype="step",lw=2,label="Merging")
plt.yscale("log")
plt.legend(title='Compactness Patton(2020)')
#plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
plt.ylabel("Number",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)

plt.subplot(5,2,6)
plt.scatter(nocsi_AllBHzams,dfAllBH,zorder=1,edgecolor="k",s=30,label="All")
plt.scatter(nocsi_BoundBHzams,dfBound,zorder=2,edgecolor="k",s=30, label="Bounded")
plt.scatter(nocsi_MergingBHzams,dfMerg,zorder=3,edgecolor="k",s=30, label="Merging")
plt.plot(np.linspace(0,140),np.linspace(0,140),ls="dashed",c="gray")
plt.scatter(nocsi_S_AllBHzams,nocsi_S_AllBH,zorder=4,marker='d',s=2,label='SSE')
plt.yscale("log")
plt.xscale('log')
plt.ylabel('BH mass [$M_{\odot}$]',fontsize=18)
plt.legend(title='Compactness Patton(2020)')
plt.gca().tick_params(axis='both', which='major', labelsize=18)

#Compactness xi = 0.2
plt.subplot(5,2,7)
plt.hist(o2_dfAllBH,bins=mybins,histtype="step",lw=2,label='All')
plt.hist(o2_dfBound,bins=mybins,histtype="step",lw=2,label="Bounded")
plt.hist(o2_dfMerg,bins=mybins,histtype="step",lw=2,label="Merging")
plt.yscale('log')
plt.legend(title=r'Compactness $\xi = 0.2$')
#plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
plt.ylabel("Number",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)

plt.subplot(5,2,8)
plt.scatter(o2_AllBHzams,o2_dfAllBH,zorder=1,edgecolor="k",s=30,label="All")
plt.scatter(o2_BoundBHzams,o2_dfBound,zorder=2,edgecolor="k",s=30, label="Bounded")
plt.scatter(o2_MergingBHzams,o2_dfMerg,zorder=3,edgecolor="k",s=30, label="Merging")
plt.plot(np.linspace(0,140),np.linspace(0,140),ls="dashed",c="gray")
plt.scatter(o2_S_AllBHzams,o2_S_AllBH,zorder=4,marker='d',s=2,label='SSE')
plt.yscale("log")
plt.xscale('log')
plt.ylabel('BH mass [$M_{\odot}$]',fontsize=18)
plt.legend(title=r'Compactness $\xi = 0.2$')
plt.gca().tick_params(axis='both', which='major', labelsize=18)


#Compactness xi = 0.4
plt.subplot(5,2,9)
plt.hist(o4_dfAllBH,bins=mybins,histtype="step",lw=2,label='All')
plt.hist(o4_dfBound,bins=mybins,histtype="step",lw=2,label="Bounded")
plt.hist(o4_dfMerg,bins=mybins,histtype="step",lw=2,label="Merging")
plt.yscale("log")
plt.legend(title=r'Compactness $\xi = 0.4$')
plt.xlabel("BH mass [M$_\odot$]",fontsize=18)
plt.ylabel("Number",fontsize=18)
plt.gca().tick_params(axis='both', which='major', labelsize=18)

plt.subplot(5,2,10)
plt.scatter(o4_AllBHzams,o4_dfAllBH,zorder=1,edgecolor="k",s=30,label="All")
plt.scatter(o4_BoundBHzams,o4_dfBound,zorder=2,edgecolor="k",s=30, label="Bounded")
plt.scatter(o4_MergingBHzams,o4_dfMerg,zorder=3,edgecolor="k",s=30, label="Merging")
plt.plot(np.linspace(0,140),np.linspace(0,140),ls="dashed",c="gray")
plt.scatter(o4_S_AllBHzams,o4_S_AllBH,zorder=4,marker='d',s=2,label='SSE')
plt.yscale("log")
plt.xscale('log')
plt.xlabel(r'M$_{ZAMS}$'+'[M$_{\odot}$]',fontsize=18)
plt.ylabel('BH mass [$M_{\odot}$]',fontsize=18)
plt.legend(title=r'Compactness $\xi = 0.4$')
plt.gca().tick_params(axis='both', which='major', labelsize=18)


#plt.tight_layout()
#plt.savefig("onlyZamsC.pdf")
plt.show()