build: requirements.txt
	@echo "CREATING virtual environment venv...";
	@python3 -m venv venv;
	@echo "INSTALLING dependencies...";
	@. venv/bin/activate; pip install -r requirements.txt;
	@echo "STARTING Docker containers..."
	@docker-compose up -d


cluster: 
	@echo "CREATING cluster...";
	@. venv/bin/activate; python ./scripts/create_cluster.py;

run: 
	@echo "Starting server..."
	@echo "PLEASE OPEN: http://127.0.0.1:8000/docs"
	@echo "total number of records in the final table: http://127.0.0.1:8000/Q1"
	@echo "total number of trips started and completed on June 17th: http://127.0.0.1:8000/Q2"
	@echo "the day of the longest trip traveled: http://127.0.0.1:8000/Q3"
	@echo "the mean, standard deviation, minimum, maximum and quartiles of the distribution: http://127.0.0.1:8000/Q4"
	@echo "FOR ALL ANSWERS: http://127.0.0.1:8000/all_answers"
	@echo "TO QUERY SQL USE: POST run_query on http://127.0.0.1:8000/docs"
	@. venv/bin/activate; uvicorn app.main:app --reload > /dev/null 2>&1


clean:
	@echo "DELETING cluster...";
	@. venv/bin/activate; python ./scripts/delete_cluster.py;
	@echo "DELETING venv and pycache...";
	@rm -r venv;
	@find . | grep -E "(__pycache__|.pytest_cache)" | xargs rm -rf;
