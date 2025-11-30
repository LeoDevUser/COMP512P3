/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D'Silva; ©2024, Bettina Kemme; ©2025, Olivier Michaud
*/
import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.Stat;

public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback, AsyncCallback.DataCallback, AsyncCallback.StatCallback {
    ZooKeeper zk;
    String zkServer, pinfo;
    boolean isManager=false;
    boolean initialized=false;
	String workerName = "";
	List<String> workers = new ArrayList<>();
	ConcurrentHashMap<String, String> assignments = new ConcurrentHashMap<>();
	ConcurrentHashMap<String, String> taskToWorker = new ConcurrentHashMap<>();  // track which worker has which task
	ConcurrentLinkedQueue<String> taskQueue = new ConcurrentLinkedQueue<>();
	int timeSlice = 500; // execution timeslice (ms)

    DistProcess(String zkhost)
    {
        zkServer=zkhost;
        pinfo = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("DISTAPP : ZK Connection information : " + zkServer);
        System.out.println("DISTAPP : Process information : " + pinfo);
    }

    void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
    {
        zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
    }

	//to delete task nodes from previous sessions
	void deleteTaskNodes() {
		try {
			//delete result node if exists
			List<String> tasks = zk.getChildren("/dist14/tasks", false);
			for (String task : tasks) {
				try {
					List<String> children = zk.getChildren("/dist14/tasks/" + task, false);
					for (String child : children) {
						zk.delete("/dist14/tasks/" + task + "/" + child, -1);
					}
				} catch (NoNodeException e) {
					// Already deleted, ignore
				}
				// Then delete the task node
				try {
					zk.delete("/dist14/tasks/" + task, -1);
				} catch (NoNodeException e) {
					// Already deleted, ignore
				}
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

    void initialize()
    {
        try {
			deleteTaskNodes();
            runForManager();	// See if you can become the manager (i.e, no other manager exists)
			isManager=true;
            getTasks(); // Install monitoring on any new tasks that will be created.
            getWorkers(); // Install monitoring on any new workers that join/leave.
        } catch(NodeExistsException nee) { //node will be a worker
			isManager=false;
			try {
				workerName = zk.create("/dist14/workers/worker-", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				System.out.println("Created Node: " + workerName);
				zk.getData(workerName, this, this, ""); //watch for data change, which indicates assignment
			} catch (KeeperException kep) {
				kep.printStackTrace();
			} catch (InterruptedException iee) {
				iee.printStackTrace();
			}
		} catch(UnknownHostException uhe)
        { System.out.println(uhe); }
        catch(KeeperException ke)
        { System.out.println(ke); }
        catch(InterruptedException ie)
        { System.out.println(ie); }

        System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isManager?"manager":"worker"));

    }

    // Manager fetching task znodes...
    void getTasks()
    {
        zk.getChildren("/dist14/tasks", this, this, "TASKS");  
    }
	
    // Manager fetching worker znodes
    void getWorkers()
    {
        zk.getChildren("/dist14/workers", this, this, "WORKERS");  
    }
	
    // Try to become the manager.
    void runForManager() throws UnknownHostException, KeeperException, InterruptedException
    {
        //Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
        // This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
        zk.create("/dist14/manager", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void process(WatchedEvent e)
    {
        //Get watcher notifications.

        System.out.println("DISTAPP : Event received : " + e);

        if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
        {
            // Once we are connected, do our intialization stuff.
            if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initialized == false) 
            {
                initialize();
                initialized = true;
            }
        }

        // Manager should be notified if any new znodes are added to tasks.
        if(isManager && e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist14/tasks"))
        {
            // There has been changes to the children of the node.
            // We are going to re-install the Watch as well as request for the list of the children.
            getTasks();
        }
		
        //Manager should be notified if any new znodes are added to workers.
        if(isManager && e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist14/workers"))
        {
            // There has been changes to the children of the node.
            // We are going to re-install the Watch as well as request for the list of the children.
            getWorkers();
        }

		//Manager watches worker data changes to detect when workers become IDLE
		if (isManager && e.getType() == Watcher.Event.EventType.NodeDataChanged && e.getPath().startsWith("/dist14/workers/")) {
			String name = e.getPath().substring("/dist14/workers/".length());
			zk.getData(e.getPath(), this, this, "WORKER-STATUS:" + name);
		}

		//worker should be notified when it gets assigned a task
        if(!isManager && workerName != null && !workerName.isEmpty()) {
            if(e.getType() == Watcher.Event.EventType.NodeDataChanged && e.getPath().equals(workerName)) {
                System.out.println("WORKER: My data changed");
                zk.getData(workerName, this, this, "ASSIGN");
            }
        }
    }

    //Asynchronous callback that is invoked by the zk.setData request.
	public void processResult(int rc, String path, Object ctx, Stat stat) {
	}

	public static class AssignedData implements Serializable {
		byte[] data;
		String task;

		public AssignedData(byte[] data, String task) {
			this.data = data;
			this.task = task;
		}
	}

	public static byte[] toByteArray(Object obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.close();
		return baos.toByteArray();
	}

	public static Object fromByteArray(byte[] bytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Object obj = ois.readObject();
		ois.close();
		return obj;
	}
	
    //Asynchronous callback that is invoked by the zk.getData request.
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
		if (ctx != null && ctx.toString().startsWith("TASK-DATA:")) {
			System.out.println("MANAGER: Received task data");
			//create worker assignment
			String chosenWorker = ctx.toString().substring("TASK-DATA:".length());
			chosenWorker = chosenWorker.substring(0, chosenWorker.indexOf("task"));
			String task = ctx.toString().substring("TASK-DATA:".length());
			task = task.substring(task.indexOf("task"));
			try {
				AssignedData assigned = new AssignedData(data, task);
				byte[] new_data = toByteArray((Object) assigned);
				zk.setData("/dist14/workers/"+ chosenWorker, new_data, -1, this, "ASSIGN");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (ctx != null && ctx.toString().startsWith("WORKER-STATUS:")) {
			String workerName = ctx.toString().substring("WORKER-STATUS:".length());
			// update assignments when worker stops working on task
			if (data == null) {
				String oldTask = assignments.put(workerName, "");  // Get old value and set to IDLE
				if (oldTask != null && !oldTask.equals("")) {
					// Check if task was completed or just interrupted
					try {
						Stat resultStat = zk.exists("/dist14/tasks/" + oldTask + "/result", false);
						if (resultStat == null) {
							// Task was interrupted, not completed - remove from tracking so it can be reassigned
							taskToWorker.remove(oldTask);
							// Add back to queue if not already there
							if (!taskQueue.contains(oldTask)) {
								taskQueue.add(oldTask);
								System.out.println("MANAGER: Task " + oldTask + " interrupted, added back to queue");
							}
						} else {
							// Task was completed, just remove tracking
							taskToWorker.remove(oldTask);
							System.out.println("MANAGER: Task " + oldTask + " completed by " + workerName);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				flushQueue();
			}
		}

		if (ctx != null && ctx.toString().startsWith("ASSIGN")) {
			//send work to another thread to free up the zookeper thread
			if(data == null) return;
			System.out.println("WORKER: got an assignment!!");
			Thread workerThread = new Thread(() -> {
				DistTask dt = null;
				AssignedData assignedData = null;
				try { 
					assignedData = (AssignedData) fromByteArray(data);
					System.out.println("WORKER: Processing " + assignedData.task);
					
					// Re-construct our task object.
					ByteArrayInputStream bis = new ByteArrayInputStream(assignedData.data);
					ObjectInput in = new ObjectInputStream(bis);
					dt = (DistTask) in.readObject();

					//Execute the task.
					dt.compute();
				
					// Serialize our Task object back to a byte array!
					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					ObjectOutputStream oos = new ObjectOutputStream(bos);
					oos.writeObject(dt); oos.flush();
					byte[] new_data = bos.toByteArray();

					// Store it inside the result node.
					try {
						zk.create("/dist14/tasks/"+assignedData.task+"/result",new_data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						System.out.println("WORKER: FINISHED PROCESSING " + assignedData.task);
					} catch (NodeExistsException nee) {
						System.out.println("WORKER: Result already exists for " + assignedData.task + ", another worker finished it");
					}
					
					// Worker sets itself to IDLE after completion
					zk.setData(workerName, null, -1);
					System.out.println("WORKER: Set status back to IDLE");
					
				} catch (InterruptedException ie) {
					System.out.println("WORKER: Task interrupted by time slice");
					try {
						// Check if task node still exists before updating
						Stat taskStat = zk.exists("/dist14/tasks/" + assignedData.task, false);
						if (taskStat != null) {
							// save updated task
							ByteArrayOutputStream bos = new ByteArrayOutputStream();
							ObjectOutputStream oos = new ObjectOutputStream(bos);
							oos.writeObject(dt); oos.flush();
							byte[] task_data = bos.toByteArray();

							zk.setData("/dist14/tasks/" + assignedData.task, task_data, -1);
							System.out.println("WORKER: Saved progress for " + assignedData.task);
						} else {
							System.out.println("WORKER: Task node deleted, skipping update for " + assignedData.task);
						}

						//worker sets itself to IDLE after interruption too
						zk.setData(workerName, null, -1);
						System.out.println("WORKER: Set status back to IDLE after interruption");
					} catch(Exception ee) {
						ee.printStackTrace();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			});

			workerThread.start();

			// time slice thread
			new Thread(() -> {
				try {
					Thread.sleep(timeSlice);
					if (workerThread.isAlive()) {
						workerThread.interrupt();
					}
				} catch(InterruptedException ie) {
					ie.printStackTrace();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}).start();
		}
	}

    //Asynchronous callback that is invoked by the zk.getChildren request.
    public void processResult(int rc, String path, Object ctx, List<String> children) {
        System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
		if (ctx != null && ctx.equals("TASKS")) {
			for(String task: children){
				//check if task already has result
				try {
					Stat stat = zk.exists("/dist14/tasks/" + task + "/result", false);
					if(stat != null) {
						System.out.println("MANAGER: Task " + task + " already done, skipping");
						continue;
					}
				} catch(Exception e) {}

				//check if task is already assigned
				if (taskToWorker.containsKey(task)) {
					System.out.println("MANAGER: Task " + task + " already assigned to " + taskToWorker.get(task) + ", skipping");
					continue;
				}

				//find IDLE worker by checking their data
				String assignedWorker = null;
				for(String worker : workers) {
					if(assignments.get(worker) != null && assignments.get(worker).equals("")) {
						assignedWorker = worker;
						assignments.put(worker, task);
						taskToWorker.put(task, worker);  // Track the assignment
						break;
					}
				}

				if(assignedWorker != null) {
					zk.getData("/dist14/tasks/" + task, false, this, "TASK-DATA:" + assignedWorker + task);
					System.out.println("MANAGER: Assigned Task " + task + " to worker " + assignedWorker);
				} else {
					System.out.println("MANAGER: No IDLE workers for task " + task + ", added to queue");
					if (!taskQueue.contains(task)) {taskQueue.add(task);}
				}
			}
		}
		if (ctx != null && ctx.equals("WORKERS")) {
			System.out.println("MANAGER: Available workers: " + children.size());
			for(String worker : children) {
				if(!assignments.containsKey(worker)) {
					assignments.put(worker, "");
				}
				System.out.println("  - " + worker);
				zk.getData("/dist14/workers/" + worker, this, this, "WORKER-STATUS:" + worker);
			}
			workers = new ArrayList<>(children);
			flushQueue();
		}
    }

	private synchronized void flushQueue() {
		for (String worker: workers) {
			if (assignments.get(worker) != null && assignments.get(worker).equals("") && !taskQueue.isEmpty()) {
				String task = taskQueue.poll();
				if (task == null) continue;

				//double-check task isn't already assigned
				if (taskToWorker.containsKey(task)) {
					System.out.println("MANAGER: Task " + task + " already assigned (from queue), skipping");
					continue;
				}

				// check if result exists already
				try {
					Stat stat = zk.exists("/dist14/tasks/" + task + "/result", false);
					if(stat != null) {
						continue;
					}
            	} catch(Exception e) {}

				assignments.put(worker, task);
				taskToWorker.put(task, worker);  // Track the assignment
				zk.getData("/dist14/tasks/" + task, false, this, "TASK-DATA:" + worker + task);
				System.out.println("MANAGER: Assigned Task " + task + " to worker " + worker + " from queue");
			}
		}
	}

    public static void main(String args[]) throws Exception
    {
        //Create a new process
        //Read the ZooKeeper ensemble information from the environment variable.
        DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
        dt.startProcess();

        while(true) {
			Thread.sleep(10000);
		}
    }
}
