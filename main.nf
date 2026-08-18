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

workflow {
    main:
    log.info("++++++++++========================================")
    log.info("Executing MS-QC workflow")
    log.info("")
    log.info("++++++++++========================================")

    convert(params.raw_folder,
	    params.conv_params,
	    params.monitor.toBoolean(),
	    params.link_files.toBoolean()
    )
    
    convertMzxmlW(convert.out.conv_out,
		  params.conv_params_msconvert,
		  params.link_files.toBoolean()
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
    search(params.tools_folder,
	   params.diann,
	   params.python,
	   file(params.workflow_fp),
	   ch_search_inputs.manifests,
           ch_search_inputs.mzml_files,
	   file(params.database_fp),
	   params.fragpipe_threads.toInteger())

    papermill(file(params.template_ipynb),
	      convert.out.raw_files,
	      convert.out.conv_out,
	      search.out.psm,
	      params.metrics_db
    )

    ipynb = papermill.out.ipynb
    ipynbToHtml(ipynb,
		// We add the mzXML files not because the process
		// needs them, but because we use the filename to
		// calculate the publishDir location
		convert.out.conv_out)
}
