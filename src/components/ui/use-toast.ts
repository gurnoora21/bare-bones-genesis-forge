
import { useToast as useHookToast } from "@/hooks/use-toast";
import { toast as hookToast } from "@/hooks/use-toast";

// Enhanced toast with structured logging
export function useToast() {
  const { toast } = useHookToast();
  
  return {
    toast: (props: any) => {
      // Log toast actions to console for debugging
      console.log(`Toast notification: ${props.title || 'Notification'}`, props);
      return toast(props);
    }
  };
}

// Enhanced toast with additional patterns for pipeline operations
export const toast = {
  ...hookToast,
  
  // Specialized toast for pipeline operations
  pipeline: (title: string, description?: string) => {
    console.log(`Pipeline toast: ${title}`, { description });
    return hookToast({
      title,
      description,
      variant: "default",
    });
  },
  
  // Success toast with logging
  success: (title: string, description?: string) => {
    console.log(`Success toast: ${title}`, { description });
    return hookToast({
      title,
      description,
      variant: "success",
    });
  },
  
  // Warning toast with logging
  warning: (title: string, description?: string) => {
    console.warn(`Warning toast: ${title}`, { description });
    return hookToast({
      title,
      description,
      variant: "warning",
    });
  },
  
  // Error toast with logging
  error: (title: string, description?: string) => {
    console.error(`Error toast: ${title}`, { description });
    return hookToast({
      title,
      description,
      variant: "destructive",
    });
  },
  
  // Queue status toast
  queueStatus: (queueName: string, status: {
    processed: number,
    pending: number,
    failed: number
  }) => {
    console.log(`Queue status toast for ${queueName}`, status);
    return hookToast({
      title: `${queueName} Status`,
      description: `Processed: ${status.processed}, Pending: ${status.pending}, Failed: ${status.failed}`,
      variant: status.failed > 0 ? "warning" : "default",
    });
  }
};
