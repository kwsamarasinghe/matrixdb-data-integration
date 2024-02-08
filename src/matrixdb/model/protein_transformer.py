def convert_uniprot(uniprot_entry):
    biomolecule = dict()

    if type(uniprot_entry["accession"]) is list:
        biomolecule["id"] = uniprot_entry["accession"][0]["text"]
    else:
        biomolecule["id"] = uniprot_entry["accession"]["text"]

    # print(biomolecule["id"])
    biomolecule["type"] = 'protein'
    biomolecule["dataset"] = uniprot_entry["dataset"]

    # Names
    names = dict()
    names["name"] = uniprot_entry["name"]["text"]
    names["common_name"] = uniprot_entry["name"]["text"]
    names["recommended_name"] = uniprot_entry["protein"]["recommendedName"]["fullName"]["text"]
    names["other_name"] = []
    if "alternativeName" in uniprot_entry["protein"]:
        if type(uniprot_entry["protein"]["alternativeName"]) is list:
            for name in uniprot_entry["protein"]["alternativeName"]:
                names["other_name"].append(name["fullName"]["text"])
        else:
            names["other_name"].append(uniprot_entry["protein"]["alternativeName"]["fullName"]["text"])
    biomolecule["names"] = names

    biomolecule["species"] = dict()
    biomolecule["species"]["db"] = uniprot_entry["organism"]["dbReference"]["type"]
    biomolecule["species"]["id"] = uniprot_entry["organism"]["dbReference"]["id"]

    # Relations
    relations = dict()
    if "gene" in uniprot_entry:
        if type(uniprot_entry["gene"]) is list:
            for n in uniprot_entry["gene"]:
                if type(n["name"]) == list:
                    relations["gene_name"] = list(n["text"] for n in n["name"])
                else:
                    relations["gene_name"] = n["name"]["text"]
        else:
            if type(uniprot_entry["gene"]["name"]) is list:
                relations["gene_name"] = list(n["text"] for n in uniprot_entry["gene"]["name"])
            else:
                relations["gene_name"] = uniprot_entry["gene"]["name"]["text"]
    biomolecule["relations"] = relations

    # Molecular details
    molecular_details = dict()
    if "mass" in uniprot_entry["sequence"]:
        molecular_details["molecular_weight"] = uniprot_entry["sequence"]["mass"]
    if "text" in uniprot_entry["sequence"]:
        molecular_details["sequence"] = uniprot_entry["sequence"]["text"]
    if "length" in uniprot_entry["sequence"]:
        molecular_details["sequence_length"] = uniprot_entry["sequence"]["length"]

    for pdb in list(filter(lambda c: c["type"] == "PDB", uniprot_entry["dbReference"])):
        molecular_details["pdb"] = list()
        molecular_details["pdb"].append(pdb["id"])
    biomolecule["molecular_details"] = molecular_details

    source_details = dict()
    source_details["entry_version"] = uniprot_entry["version"]
    if "version" in uniprot_entry["sequence"]:
        source_details["sequence_version"] = uniprot_entry["sequence"]["version"]
    biomolecule["source_details"] = source_details

    biomolecule["publication"] = list()
    if type(uniprot_entry["reference"]) is list:
        for reference in uniprot_entry["reference"]:
            if "dbReference" in reference["citation"]:
                if type(reference["citation"]["dbReference"]) is dict:
                    ref_type = reference["citation"]["dbReference"]["type"]
                    ref_id = reference["citation"]["dbReference"]["id"]

                    biomolecule["publication"].append({
                        "db": ref_type,
                        "id": ref_id
                    })
                else:
                    for reference_item in reference["citation"]["dbReference"]:
                        reference_item_type = reference_item["type"]
                        reference_item_id = reference_item["id"]

                        if reference_item_type == "pubmed":
                            biomolecule["publication"].append({
                                "db": reference_item_type,
                                "id": reference_item_id
                            })
    else:
        reference = uniprot_entry["reference"]
        if "dbReference" in reference:
            if type(reference["citation"]["dbReference"]) is dict:
                ref_type = reference["citation"]["dbReference"]["type"]
                ref_id = reference["citation"]["dbReference"]["id"]

                biomolecule["publication"].append({
                    "db": ref_type,
                    "id": ref_id
                })
            else:
                for reference_item in reference["citation"]["dbReference"]:
                    reference_item_type = reference_item["type"]
                    reference_item_id = reference_item["id"]

                    if reference_item_type == "pubmed":
                        biomolecule["publication"].append({
                            "db": reference_item_type,
                            "id": reference_item_id
                        })

    annotations = dict()
    if "comment" in uniprot_entry and type(uniprot_entry["comment"]) == list:
        funtion_list = list(filter(lambda c: c["type"] == "function", uniprot_entry["comment"]))
        if len(funtion_list) > 0:
            annotations["function"] = funtion_list[0]["text"]
        annotations["disease"] = list()
        for d in list(filter(lambda c: c["type"] == "disease", uniprot_entry["comment"])):
            if "disease" in d:
                annotations["disease"].append({
                    "id": d["disease"]["id"],
                    "name": d["disease"]["name"]["text"],
                    "description": d["disease"]["description"]["text"]
                })
            else:
                if "text" in d:
                    annotations["disease"].append({
                        "description": d["text"]
                    })
        # annotations["ordered_locus_name"]
        tissue_spec_list = list(filter(lambda c: c["type"] == "tissue specificity", uniprot_entry["comment"]))
        if len(tissue_spec_list) > 0:
            annotations["tissue_specificity"] = tissue_spec_list[0]

        subcell_list = list(filter(lambda c: c["type"] == "subcellular location", uniprot_entry["comment"]))
        subcell_comments = list()
        if len(subcell_list) > 0:
            for subcell_comment in subcell_list:
                if "subcellularLocation" in subcell_comment:
                    if type(subcell_comment["subcellularLocation"]) == list:
                        for l in subcell_comment["subcellularLocation"]:
                            if type(l["location"]) is dict:
                                subcell_comments.append(l["location"]["text"])
                            else:
                                for ll in l["location"]:
                                    subcell_comments.append(ll["text"])
                    else:
                        if type(subcell_comment["subcellularLocation"]["location"]) == list:
                            for ll in subcell_comment["subcellularLocation"]["location"]:
                                subcell_comments.append(ll["text"])
                        else:
                            subcell_comments.append(subcell_comment["subcellularLocation"]["location"]["text"])

        annotations["subcellular_location"] = []
        for subcell_comment in subcell_comments:
            annotations["subcellular_location"].append(subcell_comment)

    annotations["keywords"] = list()
    for keyword in uniprot_entry["keyword"]:
        if type(keyword) is dict:
            if 'id' in keyword:
                annotations["keywords"].append(keyword['id'])
            else:
                print('Should handle')

    go_terms = list(go_term['id'] for go_term in list(filter(lambda c: c["type"] == "GO", uniprot_entry["dbReference"])))
    if len(go_terms) > 0:
        annotations["go"] = go_terms
    biomolecule["annotations"] = annotations

    xrefs = {}
    if type(uniprot_entry["accession"]) is list:
        xrefs["uniprot"] = uniprot_entry["accession"][0]["text"]
    else:
        xrefs["uniprot"] = uniprot_entry["accession"]["text"]
    reactome_refs = list(filter(lambda c: c["type"] == "Reactome", uniprot_entry["dbReference"]))
    if len(reactome_refs) > 0:
        xrefs["reactome"] = list({'id': rf['id'], 'type': rf['property']['type'], 'value': rf['property']['value']}for rf in reactome_refs)
    xrefs["disgenet"] = list(
        d["id"] for d in list(filter(lambda c: c["type"] == "DisGeNET", uniprot_entry["dbReference"])))
    xrefs["interpro"] = list()
    for ip in list(filter(lambda c: c["type"] == "InterPro", uniprot_entry["dbReference"])):
        xrefs["interpro"].append(ip["id"])

    xrefs["pfam"] = list()
    for pfam in list(filter(lambda c: c["type"] == "Pfam", uniprot_entry["dbReference"])):
        xrefs["interpro"].append(pfam["id"])

    xrefs["alpha_fold"] = list()
    for af in list(filter(lambda c: c["type"] == "AlphaFoldDB", uniprot_entry["dbReference"])):
        xrefs["alpha_fold"].append(af["id"])

    biomolecule["xrefs"] = xrefs
    return biomolecule


def convert_trembl(trembl_entry):
    biomolecule = dict()

    # Safe value access using get()
    biomolecule["id"] = trembl_entry["primaryAccession"]
    biomolecule["type"] = 'protein'
    biomolecule["dataset"] = trembl_entry["entryType"]

    # Names
    names = dict()
    if "recommendedName" in trembl_entry["proteinDescription"]:
        names["name"] = trembl_entry["proteinDescription"]["recommendedName"]["fullName"]["value"]
        names["recommended_name"] = trembl_entry["proteinDescription"]["recommendedName"]["fullName"]["value"]
        names["other_name"] = []

    if "proteinDescription" in trembl_entry and "includes" in trembl_entry["proteinDescription"]:
        if type(trembl_entry["proteinDescription"]["includes"]) is list:
            for other_names in trembl_entry["proteinDescription"]["includes"]:
                if "recommendedName" in other_names:
                    names["other_name"].append(other_names["recommendedName"])
                if "alternativeNames" in other_names:
                    for alt_name in other_names["alternativeNames"]:
                        if "fullName" in alt_name:
                            names["other_name"].append(alt_name["fullName"]["value"])
    biomolecule["names"] = names

    biomolecule["species"] = dict()
    biomolecule["species"]["db"] = "NCBI Taxonomy"
    biomolecule["species"]["id"] = trembl_entry["organism"]["taxonId"]

    # Relations
    relations = dict()
    if "genes" in trembl_entry:
        if type(trembl_entry["genes"]) is list:
            relations["gene_name"] = list()
            for n in trembl_entry["genes"]:
                if "geneName" in n:
                    relations["gene_name"].append(n["geneName"]["value"])
        else:
            if "geneName" in trembl_entry["gene"]:
                relations["gene_name"] = trembl_entry["gene"]["geneName"]["value"]
    biomolecule["relations"] = relations

    # Molecular details
    molecular_details = {}
    molecular_details["molecular_weight"] = trembl_entry["sequence"]["molWeight"]
    molecular_details["sequence"] = trembl_entry["sequence"]["value"]
    molecular_details["sequence_length"] = trembl_entry["sequence"]["length"]
    biomolecule["molecular_details"] = molecular_details

    source_details = dict()
    source_details["entry_version"] = trembl_entry["entryAudit"]["entryVersion"]
    source_details["sequence_version"] = trembl_entry["entryAudit"]["sequenceVersion"]
    biomolecule["source_details"] = source_details

    biomolecule["publication"] = list()
    for reference in trembl_entry["references"]:
        if "citationCrossReferences" in reference["citation"]:
            if type(reference["citation"]["citationCrossReferences"]) is dict:
                ref_type = reference["citation"]["citationCrossReferences"]["database"]
                ref_id = reference["citation"]["citationCrossReferences"]["id"]

                biomolecule["publication"].append({
                    "db": ref_type,
                    "id": ref_id
                })
            else:
                for reference_item in reference["citation"]["citationCrossReferences"]:
                    reference_item_type = reference_item["database"]
                    reference_item_id = reference_item["id"]

                    if reference_item_type == "pubmed":
                        biomolecule["publication"].append({
                            "db": reference_item_type,
                            "id": reference_item_id
                        })

    annotations = dict()
    if "features" in trembl_entry:
        function_list = list(filter(lambda c: c["type"] == "function", trembl_entry["features"]))
        if len(function_list) > 0:
            annotations["function"] = function_list[0]["text"]
        annotations["disease"] = list({
                                          "id": d["disease"]["id"],
                                          "name": d["disease"]["name"]["text"],
                                          "description": d["disease"]["description"]["text"]
                                      }
                                      for d in
                                      list(filter(lambda c: c["type"] == "disease", trembl_entry["features"])))

        # annotations["ordered_locus_name"]
        tissue_spec_list = list(filter(lambda c: c["type"] == "tissue specificity", trembl_entry["features"]))
        if len(tissue_spec_list) > 0:
            annotations["tissue_specificity"] = tissue_spec_list[0]
        subcell_comments = list(comment["subcellularLocation"] for comment in
                                list(filter(lambda c: c["type"] == "subcellular location",
                                            trembl_entry["features"])))
        annotations["subcellular_location"] = []
        for subcell_comment in subcell_comments:
            if type(subcell_comment) is list:
                for subcell_location in subcell_comment:
                    # Major hack need to fix the parser
                    if type(subcell_location["location"]) is dict:
                        annotations["subcellular_location"].append(subcell_location["location"]["text"])
                    else:
                        for l in subcell_location["location"]:
                            annotations["subcellular_location"].append(l["text"])
            else:
                annotations["subcellular_location"].append(subcell_comment["location"]["text"])

    if "keywords" in trembl_entry:
        annotations["keywords"] = trembl_entry["keywords"]
    annotations["go"] = list()
    biomolecule["annotations"] = annotations

    xrefs = {}
    xrefs["interpro"] = list()
    for xref in trembl_entry["uniProtKBCrossReferences"]:
        if xref["database"] == "KEGG":
            xrefs["kegg"] = xref["id"]

        if xref["database"] == "InterPro":
            xrefs["interpro"].append(xref["id"])

        if xref["database"] == "Pfam":
            xrefs["interpro"].append(xref["id"])

        if xref["database"] == "GO":
            annotations["go"].append(xref["id"])

    biomolecule["xrefs"] = xrefs
    return biomolecule