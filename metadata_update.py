import requests
import os
from datetime import datetime
import pandas as pd

scan_dict = {
    "anatomical": "anat",
    "structural": "anat",
    "functional": "func",
    "behavioral": "beh",
    "diffusion": "dwi",
    "perfusion": "perf",
}
age_dict = {
    (0, 10): "0-10",
    (11, 17): "11-17",
    (18, 25): "18-25",
    (26, 34): "26-34",
    (35, 50): "35-50",
    (51, 65): "51-65",
    (66, 1000): "66+",
}
bool_dict = {True: "yes", False: "no", None: "no"}
date_arg_format = "%Y-%m-%d"
date_input_format = "%Y-%m-%d"
date_output_format = "%Y-%m-%d"

headers = {
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Connection": "keep-alive",
    "DNT": "1",
    "Origin": "https://openneuro.org",
}


def format_modalities(all_modalities):
    modalities_available_list = []
    if any(("MRI_" in e for e in all_modalities)):
        all_modalities.remove("MRI")
        for m in all_modalities:
            if "MRI" in m:
                scan_type = scan_dict[m.split("MRI_", 1)[1].lower()]
                new_m = "MRI - " + scan_type
                modalities_available_list.append(new_m)
            else:
                modalities_available_list.append(m)
    else:
        modalities_available_list = all_modalities
    return ", ".join(modalities_available_list)


def format_ages(raw_age_list):
    formatted_list = []
    if raw_age_list:
        age_list = sorted([x["age"] for x in raw_age_list if x["age"]])
        for key, value in age_dict.items():
            if any(x for x in age_list if x >= key[0] and x <= key[1]):
                formatted_list.append(value)
        return ", ".join(formatted_list)
    else:
        return ""


def format_name(name):
    if not name:
        return ""
    elif "," not in name:
        last = name.split(" ")[-1]
        first = " ".join(name.split(" ")[0:-1])
        new_name = last + ", " + first
        return new_name
    else:
        return name
        
def code_bool(parent, field):
    if field in parent:
        if parent[field]:
            return "Yes"
        elif parent[field] is False:
            return "No"
        elif parent[field] is None:
            return "n/a"
    else:
        return "n/a"

query = """
{
    edges {
        cursor,
        node {
            id,
            publishDate,
            latestSnapshot {
                tag,
                created,
                size,
                dataset {
                    name, 
                    metadata {
                      trialCount,
                      studyDesign,
                      studyDomain,
                      studyLongitudinal,
                      dataProcessed,
                      species,
                      associatedPaperDOI,
                      openneuroPaperDOI,
                      dxStatus,
                      affirmedConsent,
                      affirmedDefaced
                    }
                }, 
                description {
                    SeniorAuthor
                },
                summary {
                    subjects,
                    modalities, 
                    secondaryModalities, 
                    subjectMetadata {
                        age
                    }, 
                    tasks,
                    dataProcessed
                }
            }
        }
    }
}
""".replace("\n", "")
data = '{"query":"query testq{datasets ' + query + '}"}'

response = requests.post("https://openneuro.org/crn/graphql", headers=headers, data=data)
response = response.json()

output = []
# remember to remove duplicates
while True:
    for ds in response["data"]["datasets"]["edges"]:
        if not ds:
            continue
        dataset_field = ds["node"]["latestSnapshot"]["dataset"]
        summary_field = ds["node"]["latestSnapshot"]["summary"]
        accession_number = ds["node"]["id"]
        
        try:
            dataset_made_public_datetime = datetime.strptime(
                ds["node"]["publishDate"][:10], date_input_format
            )
            dataset_made_public = dataset_made_public_datetime.strftime(date_output_format)
        except TypeError:
            dataset_made_public_datetime = None
        dataset_url = os.path.join(
            "https://openneuro.org/datasets/",
            accession_number,
            "versions",
            ds["node"]["latestSnapshot"]["tag"],
        )
        dataset_name = dataset_field["name"]
        most_recent_snapshot_date = datetime.strptime(
            ds["node"]["latestSnapshot"]["created"][:10],
            date_input_format,
        ).strftime(date_output_format)
        if ds["node"]["latestSnapshot"]["size"]:
            size_gb = round(ds["node"]["latestSnapshot"]["size"] / (1024**3), 2)
        else:
            size_gb = None
        if summary_field is not None:
            number_of_subjects = str(len(summary_field["subjects"]))
            modalities_available = format_modalities(
                summary_field["secondaryModalities"] + summary_field["modalities"]
            )
            ages = format_ages(summary_field["subjectMetadata"])
            tasks_completed = ", ".join(summary_field["tasks"])
        dx_status = dataset_field["metadata"]["dxStatus"]
        number_of_trials = dataset_field["metadata"]["trialCount"]
        study_design = dataset_field["metadata"]["studyDesign"]
        domain_studied = dataset_field["metadata"]["studyDomain"]
        longitudinal = (
            "Yes" if dataset_field["metadata"]["studyLongitudinal"] == "Longitudinal" else "No"
        )
        processed_data = code_bool(dataset_field["metadata"],"dataProcessed")
        species = dataset_field["metadata"]["species"]
        nondefaced_consent =  code_bool(dataset_field["metadata"],"affirmedConsent")
        affirmed_defaced = code_bool(dataset_field["metadata"],"affirmedDefaced")
        doi_of_paper_associated_with_ds = dataset_field["metadata"]["associatedPaperDOI"]
        doi_of_paper_because_ds_on_openneuro = dataset_field["metadata"]["openneuroPaperDOI"]
        senior_author = format_name(ds["node"]["latestSnapshot"]["description"]["SeniorAuthor"])
        line_raw = [
            accession_number,
            dataset_url,
            dataset_name,
            dataset_made_public,
            most_recent_snapshot_date,
            number_of_subjects,
            modalities_available,
            dx_status,
            ages,
            tasks_completed,
            number_of_trials,
            study_design,
            domain_studied,
            longitudinal,
            processed_data,
            species,
            nondefaced_consent,
            affirmed_defaced,
            doi_of_paper_associated_with_ds,
            doi_of_paper_because_ds_on_openneuro,
            senior_author,
            size_gb
        ]
        line = ["" if x is None else str(x) for x in line_raw]
        output.append(line)

    if len(response["data"]["datasets"]["edges"]) < 25:
        break

    next_cur = ds["cursor"]
    data = f'{{"query": "query testq{{datasets(after: \\"{next_cur}\\") ' + query + '}"}'
    response = requests.post(
        "https://openneuro.org/crn/graphql",
        headers=headers,
        data=data,
    )
    response = response.json()

header = [
    "accession_number",
    "dataset_url",
    "dataset_name",
    "made_public",
    "most_recent_snapshot",
    "num_subjects",
    "modalities",
    "dx_status",
    "ages",
    "tasks",
    "num_trials",
    "study_design",
    "domain_studied",
    "longitudinal",
    "processed_data",
    "species",
    "nondefaced_consent",
    "affirmed_defaced",
    "doi_of_papers_from_source_data_lab",
    "doi_of_paper_published_using_openneuro_dataset",
    "senior_author",
    "size_gb"
]
df = pd.DataFrame(output, columns=header)
df = df.set_index("accession_number")
df = df.sort_index()
df = df.groupby(df.index).first()
df.to_csv('metadata.csv', mode='w+')


