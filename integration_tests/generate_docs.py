import os



def generate_docs():

    print ("Run dbt generate docs")
    
    run_generate_docs = "dbt docs generate"
    os.system(run_generate_docs)

    print ("Copying files to be servable")

    files_dir = 'target'
    file_names = [
        'catalog.json',
        'index.html',
        'manifest.json',
        'run_results.json'
    ]

    for file_name in file_names:

        cp_cmd = 'cp {} docs'.format(os.path.join(files_dir, file_name))
        os.system(cp_cmd)

    
    print ("Copying files complete")

if __name__ == "__main__":
    generate_docs()