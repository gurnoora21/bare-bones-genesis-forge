
import { createBrowserRouter } from "react-router-dom";
import Index from "./pages/Index";
import NotFound from "./pages/NotFound";
import MusicDiscovery from "./pages/MusicDiscovery";

export const router = createBrowserRouter([
  {
    path: "/",
    element: <Index />,
  },
  {
    path: "/music-discovery",
    element: <MusicDiscovery />,
  },
  {
    path: "*",
    element: <NotFound />,
  },
]);
