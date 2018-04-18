# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 02:25:18 2018

@author: Entropic Turnips
"""

sepsis_pred = {}
martin = {}
angus = {}
explicit = {}
with open('septic_id_timestamp.csv') as my_septic_id:
    content = my_septic_id.readlines()
    sepsis_pred = {x.split(",")[0] : x.split(",")[1] for x in content}

with open('angus_sepsis.csv') as file:
    content = file.readlines()
    angus = {x.split(",")[0] : x.split(",")[1] for x in content}

with open('explicit_sepsis.csv') as file:
    content = file.readlines()
    explicit = {x.split(",")[0] : x.split(",")[1] for x in content}
    
with open('martin_sepsis.csv') as file:
    content = file.readlines()
    martin = {x.split(",")[0] : x.split(",")[1] for x in content}
    
    
martin_pred = [0., 0., 0.]
angus_pred = [0., 0., 0.]
exp_pred = [0., 0., 0.]
martin_angus = [0., 0., 0.]
martin_exp = [0., 0., 0.]
angus_exp = [0., 0., 0.]

def compare(pred, source, conf_mat):
    total = 0
    for key in pred:
        if (key in source):
            conf_mat[0] += 1
            total += 1
        else:
            conf_mat[1] += 1
            total += 1
    for key in source:
        if (key not in pred):
            conf_mat[2] += 1
            total += 1
    conf_mat[0] /= float(total)
    conf_mat[1] /= float(total)
    conf_mat[2] /= float(total)


compare(sepsis_pred, martin, martin_pred)
compare(sepsis_pred, angus, angus_pred)
compare(sepsis_pred, explicit, exp_pred)
compare(martin, angus, martin_angus)
compare(martin, explicit, martin_exp)
compare(angus, explicit, angus_exp)    

print('Martin Pred:\n', martin_pred)
print('Angus Pred:\n', angus_pred)
print('Explicit Pred:\n', exp_pred)
print('Martin Angus:\n', martin_angus)
print('Martin Explicit:\n', martin_exp)
print('Angus Explicit:\n', angus_exp)  

