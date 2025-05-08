
import { RouterProvider } from "react-router-dom";
import { router } from "./routes";
import { Toaster } from "./components/ui/sonner";
import "./App.css";

function App() {
  return (
    <main>
      <RouterProvider router={router} />
      <Toaster />
    </main>
  );
}

export default App;
