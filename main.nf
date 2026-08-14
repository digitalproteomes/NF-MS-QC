nextflow.enable.dsl=2

include {convert;
	 convertMzxmlW} from './NF-ConvertThermo/convertThermo_workflows.nf'

include {search} from './NF-Frapipe/fragpipe_workflows.nf'

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

    // TODO: This needs to be adjusted since we don't have unique
    // manifest-fp.manifest in this implementation

    // NOTE: the manifest-fp.manifest is currently being generate by a
    // shell script. We should write a nexflow process to do it. Since
    // this is running as a single pipeline this will end up in the
    // work directory and we won't have to worry about naming
    // collisions.

    // REP=0
    // find "$(pwd)/Results/MzML" -name "*.mzML" -print0 | while IFS= read -r -d '' FILE; do
    //   ((REP++))
    //   printf "%s\tQC\t%d\t${SEARCH_TYPE}\n" "$FILE" "$REP"
    // done > manifest.fp-manifest
    
    
    // Extract the list of files we need to search from FragPipe manifest file
    channel.fromPath(params.manifest_fp)
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
	   file(params.manifest_fp), // TODO: Adjust as well
	   raw_files,
	   file(params.database_fp),
	   params.fragpipe_threads.toInteger())

}
