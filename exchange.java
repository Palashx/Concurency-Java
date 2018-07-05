import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;



public class exchange {
	
    public static void main(String[] args) {
    	String file = "calls.txt";
        HashMap<String, List<String>> hm = new HashMap<>();
        try (Stream<String> mystream = Files.lines(Paths.get(file))) {
            mystream.forEach(x -> {
            	// Splitting caller and Callee
                String[] callerandcallee = x.split(",", 2);
                String key = callerandcallee[0].replace("{", "");
                List<String> value = Arrays.asList(callerandcallee[1].replace("[", "").trim().replace("]", "").trim().replace("}", "").trim().replace(".", "").trim().split(","));
                hm.put(key, value);
            });
        } catch (IOException io) {
            io.printStackTrace();
        }
        System.out.println("** Calls to be made **");
//        Iterate map to print values
        Set<String> keys = hm.keySet();
        for(String key: keys){
            System.out.println(key+": "+hm.get(key));
        }
        
        System.out.println("\n");
      
//        LinkedBlockingQueue
        HashMap<String, Calling> secondaryThreads = new HashMap<>();
        LinkedBlockingQueue<Attributes> myqueue = new LinkedBlockingQueue<>();
        
       
        Thread primaryThread = new Thread(() -> {
        	
        	Set<String> keys2 = hm.keySet();
            for(String key: keys2){
                //System.out.println(key+": "+hm.get(key));
                Calling task = new Calling(key, new LinkedBlockingQueue<>(), myqueue, secondaryThreads);
                Thread x = new Thread(task);
                x.start();
                secondaryThreads.put(key, task);
            }
            
            Set<String> keys3 = hm.keySet();
            for(String key: keys3){
            	secondaryThreads.get(key).firstMessage(secondaryThreads, hm.get(key));
            }
        
        boolean isrunning = true;
            long init = System.currentTimeMillis();
            while (isrunning) {
                if (System.currentTimeMillis() - init > 3000) {
                    System.out.println("\nMaster has received no replies for 3 seconds, ending...");
                    isrunning = !isrunning;
                }
                while (!myqueue.isEmpty()) {
                    Attributes mesinstance = myqueue.poll();
                    System.out.println(mesinstance.getReceiver() + " received " + mesinstance.getMessageType() + " message from " + mesinstance.getSender() + " [" + mesinstance.getTimeStamp() + "]");
                    init = System.currentTimeMillis();
                }
            }
        });
        
        // Start master thread.
        primaryThread.start();
    }
}

class Attributes {

    private String sender;
    private String receiver;
    private long timestamp;
    private String msgt;

    public Attributes(String from, String to, long timeStamp, String messageType) {
        this.sender = from;
        this.receiver = to;
        this.timestamp = timeStamp;
        this.msgt = messageType;
    }

    public String getSender() {
        return sender;
    }

    public String getReceiver() {
        return receiver;
    }

    public long getTimeStamp() {
        return timestamp;
    }

    public String getMessageType() {
        return msgt;
    }

    public void setMessageType(String messageType) {
        this.msgt = messageType;
    }
}


class Calling implements Runnable {
    private String name;
    private Queue<Attributes> firstQueue;
    private Queue<Attributes> secondQueue;
    private HashMap<String, Calling> secondaryProcess;
    
    public Calling(String name, Queue<Attributes> fqueue, Queue<Attributes> squeue, HashMap<String, Calling> sprocess) {
        this.name = name;
        this.firstQueue = fqueue;
        this.secondQueue = squeue;
        this.secondaryProcess = sprocess;
    }

    public void run() {
        boolean isrunning = true;
        long init = System.currentTimeMillis();
        while (isrunning) {
            if (System.currentTimeMillis() - init > 2000) {
                System.out.println("\nProcess " + name + " has recieved no calls for 2 second, ending...");
                isrunning = !isrunning;
            }
            
            while (!firstQueue.isEmpty()) {
                Attributes attribute = firstQueue.poll();
                if (attribute.getMessageType().equals("intro")) {
                    secondMessage(attribute.getSender(), attribute.getTimeStamp());
                }
                secondQueue.add(attribute);
                init = System.currentTimeMillis();
            }
        }
    }
    
    public void firstMessage(HashMap<String, Calling> sprocess, List<String> value) {
        
    	for(int i = 0; i < value.size(); i++){
    		Calling task = sprocess.get(value.get(i));
            try {
                Thread.sleep(100);
                Attributes message = new Attributes(name, value.get(i), System.currentTimeMillis(), "intro");
                task.firstQueue.add(message);
            } catch (Exception e) {
            		
            }
    	}
    }

    public void secondMessage(String recievedfrom, long timeStamp) {
            Calling process = secondaryProcess.get(recievedfrom);
            Attributes message = new Attributes(name, recievedfrom, timeStamp, "reply");
            process.firstQueue.add(message);
    }
}

