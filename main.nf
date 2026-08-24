nextflow.enable.dsl=2

include {convert;
	 convertMzxmlW} from './NF-ConvertThermo/convertThermo_workflows.nf'

include {search} from './NF-FragPipe/fragpipe_workflows.nf'

process generateManifest {
    input:
    path mzml_files
    
    output:
    tuple path("manifest.fp-manifest"), path(mzml_files), emit: manifest_data
    
    script:
    """
    REP=0
    for FILE in ${mzml_files}; do
        REP=\$((REP + 1))
        printf "%s\\tQC\\t%d\\tDDA+\\n" "\$FILE" "\$REP"
    done > manifest.fp-manifest
    """
}

process papermill {
    tag "$template_ipynb"
    cpus 2
    memory 5.GB

    publishDir 'Results/Jhub', mode: 'copy'
    
    input:
    path template_ipynb    
    val raw_file
    path mzxml_file
    path psm_file
    path metrics_db

    output:
    path "qc_*.ipynb", emit: ipynb

    script:
    """
    MZXML_BASENAME=\$(basename "$mzxml_file")
    MZXML_BASENAME=\${MZXML_BASENAME%.*}

    papermill "$template_ipynb" "qc_\${MZXML_BASENAME}.ipynb" \
        --kernel python3_parallel \
        -p raw_file "$raw_file" \
        -p mzxml_file "$mzxml_file" \
        -p psm_file "$psm_file" \
	-p metrics_db "$metrics_db"
    """
}


process ipynbToHtml{
    tag "$ipynb"
    cpus 1
    memory 10.GB

    publishDir 'Results/Jhub', mode: 'copy'

    input:
    path ipynb
    path mzxml_file

    output:
    file '*.html'

    script:
    """
    jupyter-nbconvert --to html $ipynb
    """
}

process archiveRawFile {
    tag "$raw_file"
    container null

    input:
    val raw_file
    path html_file
    val archive_folder

    script:
    """
    mv "$raw_file" "$archive_folder/"
    """
}

workflow {
    main:
    log.info("++++++++++========================================")
    log.info("Executing MS-QC workflow")
    log.info("")
    log.info("++++++++++========================================")

    log.info groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(params))
    
    convert(params.raw_folder,
	    params.conv_params,
	    params.monitor.toBoolean(),
	    params.link_files.toBoolean()
    )

    // Match mzxml and raw files coming from convert() based on filename
    raw_ch  = convert.out.raw_files.map { file -> tuple(file.baseName, file) }
    conv_ch = convert.out.conv_out.map  { file -> tuple(file.baseName, file) }
    qc_pairs = raw_ch.join(conv_ch)

    runQc(qc_pairs.map { _id, _raw, conv -> conv },
	  params.conv_params_msconvert,
	  params.link_files.toBoolean(),
	  params.tools_folder,
	  params.diann,
	  params.python,
	  params.workflow_fp,
	  params.database_fp,
	  params.fragpipe_threads.toInteger(),
	  params.template_ipynb,
	  qc_pairs.map { _id, raw, _conv -> raw },
	  params.metrics_db
    )

    // Archive the original raw file after successful QC
    if (params.archive_raw.toBoolean()) {
        archiveRawFile(
            qc_pairs.map { _id, raw, _conv -> raw.toAbsolutePath().toString() },
            runQc.out.html,
            params.archive_folder
        )
    }
}

workflow runQc{
    take:
    mzxml_file
    conv_params_msconvert
    link_files
    tools_folder
    diann
    python
    workflow_fp
    database_fp
    fragpipe_threads
    template_ipynb_file
    raw_file
    metrics_db

    main:
    convertMzxmlW(mzxml_file,
		  conv_params_msconvert,
		  link_files
    )

    // Generate the manifest file from the converted mzML files
    generateManifest(convertMzxmlW.out)

    // Unpack the synchronized tuple into separate matched channels
    generateManifest.out.manifest_data
        .multiMap { manifest, files ->
            manifests: manifest
            mzml_files: files
        }
        .set { ch_search_inputs }

    // Run FragPipe analysis
    search(tools_folder,
	   diann,
	   python,
	   file(workflow_fp),
	   ch_search_inputs.manifests,
           ch_search_inputs.mzml_files,
	   file(database_fp),
	   fragpipe_threads)

    // TODO: you have to make sure these are all matched!
    papermill(file("$baseDir/$template_ipynb_file"),
	      raw_file,
	      mzxml_file,
	      search.out.psm,
	      metrics_db
    )

    ipynb = papermill.out.ipynb
    html_out = ipynbToHtml(ipynb,
		// We add the mzXML files not because the process
		// needs them, but because we use the filename to
		// calculate the publishDir location
		mzxml_file)

    emit:
    html = html_out
}
