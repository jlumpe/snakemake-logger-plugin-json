

example.log:
	rm -rf .workdir
	snakemake \
		-s $(realpath ../workflow/Snakefile) \
		-d .workdir \
		-c 1 \
		--logger json \
		--logger-json-validate \
		--logger-json-multiline \
		--logger-json-file example.log
