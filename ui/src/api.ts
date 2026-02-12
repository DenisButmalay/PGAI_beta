import axios from "axios";

export const api = axios.create({
  baseURL: "", // важно: используем vite proxy
});
