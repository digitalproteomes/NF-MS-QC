nextflow.enable.dsl=2

include {convert;
     convertMzxmlW} from './NF-ConvertThermo/convertThermo_workflows.nf'

include {search} from './NF-FragPipe/fragpipe_workflows.nf'

process generateManifest {
    tag "$id"

    input:
    tuple val(id), path(mzml_file)

    output:
    tuple val(id), path("manifest.fp-manifest"), path(mzml_file), emit: manifest_data

    script:
    """
    printf "%s\\t%s\\t1\\tDDA+\\n" "${mzml_file}" "${id}" > manifest.fp-manifest
    """
}

process papermill_instrument {
    tag "$id"
    cpus 2
    memory 5.GB

    publishDir 'Results/Jhub', mode: 'copy'

    input:
    path template_ipynb
    tuple val(id), val(raw_file), path(mzxml_file)

    output:
    tuple val(id), path("qc_instrument_${id}.ipynb"), emit: ipynb

    script:
    """
    papermill "$template_ipynb" "qc_instrument_${id}.ipynb" \
        --kernel python3_parallel \
        -p raw_file "$raw_file" \
        -p mzxml_file "$mzxml_file"
    """
}

process papermill_identification {
    tag "$id"
    cpus 2
    memory 5.GB

    publishDir 'Results/Jhub', mode: 'copy'

    input:
    path template_ipynb
    tuple val(id), val(raw_file), path(mzxml_file), path(psm_file)

    output:
    tuple val(id), path("qc_identification_${id}.ipynb"), emit: ipynb

    script:
    """
    papermill "$template_ipynb" "qc_identification_${id}.ipynb" \
        --kernel python3_parallel \
        -p raw_file "$raw_file" \
        -p mzxml_file "$mzxml_file" \
        -p psm_file "$psm_file"
    """
}


process ipynbToHtml{
    tag "$id"
    cpus 1
    memory 10.GB

    publishDir 'Results/Jhub', mode: 'copy'

    input:
    tuple val(id), path(ipynb)

    output:
    tuple val(id), path('*.html'), emit: html

    script:
    """
    jupyter-nbconvert --to html $ipynb
    """
}

process archiveRawFile {
    tag "$id"
    container null

    input:
    tuple val(id), val(raw_file)
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

    runQc(qc_pairs,
	  params.conv_params_msconvert,
	  params.link_files.toBoolean(),
	  params.tools_folder,
	  params.diann,
	  params.python,
	  params.workflow_fp,
	  params.database_fp,
	  params.fragpipe_threads.toInteger(),
	  params.instrument_template_ipynb,
	  params.identification_template_ipynb,
	  params.metrics_db
    )

    // Archive the original raw file after successful QC
    if (params.archive_raw.toBoolean()) {
        archiveRawFile(
            qc_pairs.map { id, raw, _conv -> tuple(id, raw.toAbsolutePath().toString()) }
                    .join(runQc.out.html.filter { _id, html -> html.name.contains('qc_identification') })
                    .map { id, raw, _html -> tuple(id, raw) },
            params.archive_folder
        )
    }
}

workflow runQc{
    take:
    qc_pairs          // tuple(id, raw, mzXML)
    conv_params_msconvert
    link_files
    tools_folder
    diann
    python
    workflow_fp
    database_fp
    fragpipe_threads
    instrument_template_ipynb_file
    identification_template_ipynb_file
    metrics_db

    main:
    // Run papermill on mzXML for instrument QC
    papermill_instrument(
	file("$baseDir/$instrument_template_ipynb_file"),
	qc_pairs
    )

    // Convert mzXML to mzML
    convertMzxmlW(qc_pairs.map { id, _raw, conv -> conv },
          conv_params_msconvert,
          link_files
    )

    // Re-key the mzML output by basename
    mzml_keyed = convertMzxmlW.out.map { f -> tuple(f.baseName, f) }

    // Generate the manifest file from the converted mzML files
    generateManifest(mzml_keyed)

    // Unpack the synchronized tuple into separate matched channels
    generateManifest.out.manifest_data
        .multiMap { id, manifest, files ->
            ids:       id
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

    // search.out.psm is now tuple(mzml, psm) from fragpipeSearch
    psm_keyed = search.out.psm.map { mzml, psm -> tuple(mzml.baseName, psm) }

    // Join qc_pairs with psm_keyed on id
    papermill_input = qc_pairs.join(psm_keyed)   // id, raw, mzXML, psm

    // Run papermill for identifications
    papermill_identification(file("$baseDir/$identification_template_ipynb_file"),
          papermill_input,
    )

    // Convert both instrument and identification notebooks to HTML
    ipynbToHtml(papermill_instrument.out.ipynb.mix(papermill_identification.out.ipynb))

    emit:
    html = ipynbToHtml.out.html   // tuple(id, html)
}
