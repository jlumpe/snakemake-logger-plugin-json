

example.log:
	rm -rf .workdir
	snakemake \
		-s $(realpath ../workflow/Snakefile) \
		-d .workdir \
		-c 1 \
		--keep-going \
		--logger json \
		--logger-json-validate \
		--logger-json-multiline \
		--logger-json-file example.log
		|| true
