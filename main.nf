nextflow.enable.dsl=2

include {convert;
	 convertMzxmlW} from './NF-ConvertThermo/convertThermo_workflows.nf'

include {search} from './NF-Frapipe/fragpipe_workflows.nf'

process generateManifest {
    input:
    path mzml_files
    
    output:
    path "manifest.fp-manifest"
    
    script:
    """
    REP=0
    for FILE in ${mzml_files}; do
        REP=\$((REP + 1))
        printf "%s\\tQC\\t%d\\n" "\$FILE" "\$REP"
    done > manifest.fp-manifest
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
    
    convertMzxmlW(convert.out,
		  params.conv_params_msconvert,
		  params.link_files.toBoolean()
    )

    // Generate the manifest file from the converted mzML files
    generateManifest(convertMzxmlW.out.collect())
    
    // Extract the list of files we need to search from the generated FragPipe manifest file
    generateManifest.out
	.splitCsv(sep: '\t')
	.map { row -> file("${row[0]}") }
	.set { file_list }

    file_list.toList()
	.set { raw_files }

    // TODO: figure out how to make sure that params.outdir_search is
    // set dynamically for this last step to something that includes
    // the RAW file name so that it will be unique
    
    // Run FragPipe analysis
    search(params.tools_folder,
	   params.diann,
	   params.python,
	   file(params.workflow_fp),
	   generateManifest.out,
	   raw_files,
	   file(params.database_fp),
	   params.fragpipe_threads.toInteger())

}
