# Local Variables:
# mode: justfile
# End:

set dotenv-load
set export

arch := `uname -m`

volums:
	docker volume create shared-data
	# docker volume inspect shared-data

test-mac:
	docker build -t data_process .
	docker-compose up -d