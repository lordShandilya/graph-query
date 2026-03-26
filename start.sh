#!/bin/bash
# start.sh - Launch both backend and frontend

echo "🚀 Starting GraphQuery System..."

# Check for .env
if [ ! -f ".env" ]; then
  echo "⚠️  No .env file found. Copying from .env.example..."
  cp .env.example .env
  echo "📝 Please edit .env and add your GEMINI_API_KEY, then run this script again."
  exit 1
fi

export $(grep -v '^#' .env | xargs)

# Generate sample data if not present
if [ ! -f "data/customers.csv" ]; then
  echo "📊 Generating sample dataset..."
  python3 scripts/generate_sample_data.py
fi

# Install backend deps
echo "📦 Installing backend dependencies..."
cd backend
pip install -r requirements.txt -q

# Start backend in background
echo "⚙️  Starting FastAPI backend on port 8000..."
python3 main.py &
BACKEND_PID=$!
cd ..

# Install frontend deps
echo "📦 Installing frontend dependencies..."
cd frontend
if [ ! -d "node_modules" ]; then
  npm install
fi

# Start frontend
echo "🎨 Starting React frontend on port 3000..."
REACT_APP_API_URL=http://localhost:8000 npm start &
FRONTEND_PID=$!
cd ..

echo ""
echo "✅ GraphQuery is running!"
echo "   Frontend: http://localhost:3000"
echo "   Backend:  http://localhost:8000"
echo "   API docs: http://localhost:8000/docs"
echo ""
echo "Press Ctrl+C to stop both servers."

# Wait and cleanup
trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; echo 'Stopped.'" EXIT
wait
