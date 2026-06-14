Build the image once:

```bash
cd /Users/rezwanhoppe-islam/data-engineering-portfolio/project-4-spreadsheet-reviewer
docker build -t spreadsheet-reviewer .
```

Then run your script:
````bash
docker run --rm \
  -v "$(pwd):/app" \
  -w /app \
  spreadsheet-reviewer \
  python parser.py
  ```