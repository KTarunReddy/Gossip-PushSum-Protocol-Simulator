package com.tarun.gossipsimulator

import scala.collection.JavaConversions.asScalaBuffer
import scala.util.Random

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala

sealed trait GossipMessages
case class Rumor(nodeName: Int,maxNode: Int,topology: String) extends GossipMessages
case object NodeCovered extends GossipMessages

object GossipSimulator{
  def main(args: Array[String]) {
    print("Input Should be of the form : numberOfNodes topology algorithm\n")
    print("Allowed values : Number of Nodes->Integer  Topology->full,line,3d,imperfect3d  Algorithm->gossip,push-sum\n\n\n")
    if(args.length!=3)
    {
      print("Invalid Number of Arguments. Number of Arguments must be equal to 3.\n")
    }
    else
    {
      var numberOfNodes = Integer.parseInt(args(0))
      var topology = args(1)
      
      val system = ActorSystem("GossipSimulator");
      if(topology.equalsIgnoreCase("3d") || topology.equalsIgnoreCase("imperfect3d"))
      {
        numberOfNodes = obtainPerfectCube(numberOfNodes)
      }
      
      val master = system.actorOf(Props(new Master(numberOfNodes, topology, args(2))), "master");
    }
    
    def obtainPerfectCube(n: Int): Int =
    {
      var cube = 1
      var i = 1
      while(i*i*i <= n)
        {
          cube = i*i*i
          i+=1
        }
      cube
    }
  }
}

class Master(numberOfNodes: Int, topology: String, algorithm: String) extends Actor{
  var actorArray: Array[ActorRef] = new Array[ActorRef](numberOfNodes)
  var completionCount = 0
  var finishTime:Long = _
  print("Number of nodes : "+numberOfNodes+"\nTopology : "+topology+"\nAlgorithm : "+algorithm);
  
  for (i <- 0 to numberOfNodes - 1) 
  {
     actorArray(i) = context.system.actorOf(Props(new Node(self,i,1)), "Node" + i.toString)
  }
  
  var initializingNode = Random.nextInt(numberOfNodes)
  var startNode = context.system.actorSelection("/user/Node" + initializingNode)
  
  if(algorithm.equalsIgnoreCase("gossip"))
  {
    startNode ! Rumor(initializingNode, numberOfNodes, topology)
  }
  
  var startTime:Long = System.currentTimeMillis();
  
  def receive = {
     case NodeCovered => {    
          completionCount = completionCount + 1
          if (completionCount == numberOfNodes) {
              finishTime=System.currentTimeMillis()
              println("\n\nGossip has successfully propogated throughout the "+ topology +" topology.")
              finishTime= finishTime-startTime
              println("Completion Time : "+finishTime+" milliSeconds")
              context.system.shutdown()
          }
        }
    
  }
}

class Node(master: ActorRef, s: Int, w: Int) extends Actor {
  def receive = {
    case Rumor(currentNode, numberOfNodes,topology) => {
      
      var rumorCounter = 0
      if(self.path.name!=sender.path.name)
          {
            rumorCounter += 1
            if (rumorCounter == 1) {            
              master ! NodeCovered
            }
          }
      
      var nextNode = 0
      var maxNode = numberOfNodes - 1
      if(topology.equalsIgnoreCase("line"))
      {
        
              if (currentNode == 0)
              {
                nextNode = currentNode + 1
              } else if (currentNode == maxNode) {
                nextNode = currentNode - 1
              } else {
                var next = Random.nextInt(2)
                if (next == 0) {
                  nextNode = currentNode + 1
                } else {
                  nextNode = currentNode - 1
                }
              }
      }
      else if(topology.equalsIgnoreCase("full"))
      {
        nextNode = Random.nextInt(numberOfNodes)
      }
      else if(topology.equalsIgnoreCase("3d"))
      {
        var possibleNodesList = new java.util.ArrayList[Int]() 
        var realNodesList = new java.util.ArrayList[Int]()
        var n = Math.cbrt(numberOfNodes).intValue()
        possibleNodesList.add(currentNode+1)
        possibleNodesList.add(currentNode-1)
        possibleNodesList.add(currentNode+n)
        possibleNodesList.add(currentNode-n)
        possibleNodesList.add(currentNode+(n*n))
        possibleNodesList.add(currentNode-(n*n))

        for(i <- 0 to maxNode)
        {
          for(j <- possibleNodesList)
            if(i == j)
            {
              realNodesList.add(i)
            }
        }
        
        var randomizer = new java.util.Random();
        nextNode = realNodesList.get(randomizer.nextInt(realNodesList.size()));
        
      }
      else if(topology.equalsIgnoreCase("imperfect3d"))
      {
        var possibleNodesList = new java.util.ArrayList[Int]() 
        var realNodesList = new java.util.ArrayList[Int]()
        var n = Math.cbrt(numberOfNodes).intValue()
        possibleNodesList.add(currentNode+1)
        possibleNodesList.add(currentNode-1)
        possibleNodesList.add(currentNode+n)
        possibleNodesList.add(currentNode-n)
        possibleNodesList.add(currentNode+(n*n))
        possibleNodesList.add(currentNode-(n*n))
        possibleNodesList.add(Random.nextInt(numberOfNodes))

        for(i <- 0 to maxNode)
        {
          for(j <- possibleNodesList)
            if(i == j)
            {
              realNodesList.add(i)
            }
        }
        
        var randomizer = new java.util.Random();
        nextNode = realNodesList.get(randomizer.nextInt(realNodesList.size()));
      }
      
      val targetNode = context.actorSelection("/user/Node" + nextNode)
      targetNode ! Rumor(nextNode, numberOfNodes, topology)
      
      if(rumorCounter <= 10)
      {
        self ! Rumor(nextNode, numberOfNodes, topology)
      }
    }
  }
} 