import matplotlib.pyplot as plt
from matplotlib_venn import venn2

with open('ecm_proteins.csv') as ecm_proteins_with_sources:
    ecm_only = 0
    matrisome_only = 0
    both = 0
    header = True
    for line in ecm_proteins_with_sources:
        if header:
            header = False
            continue

        matrixdb_ecm = line.split(',')[1]
        matrisome_ecm = line.split(',')[2]
        swissprot_entry = 'Swiss-Prot' in line.split(',')[4]

        if matrixdb_ecm == 'True' and matrisome_ecm != '':
            both += 1

        if matrixdb_ecm == 'False' and matrisome_ecm != '':
            matrisome_only += 1

        if matrixdb_ecm == 'True' and matrisome_ecm == '':
            ecm_only += 1

    print(f' matrixdb_only: {ecm_only} matrisome_only: {matrisome_only} both {both}')



# Create the Venn diagram

venn2(subsets=(ecm_only, matrisome_only, both), set_labels=('MatrixDB', 'Matrisome'))

# Show plot
plt.title("ECM Proteins based on MatrixDB and Matrisome")

plt.savefig("ecm_proteins.png")
plt.show()
