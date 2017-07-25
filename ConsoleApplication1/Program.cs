using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;

namespace ConsoleApplication1
{
    class Program
    {

        private static int _TaskNum = 10;
        private static Task[] _Tasks;
        private const int MAX_RESOURCE = 3;
        private const int RUN_LOOP = 10;
        private static SemaphoreSlim m_Semaphore;


        private static void Work1(int TaskID)
        {
            int i = 0;
            Stopwatch watch = new Stopwatch();
            var rnd = new Random();
            while (i<RUN_LOOP)
            {
                Thread.Sleep(rnd.Next(200, 500));
                Console.WriteLine("TASK       "+ TaskID +       "REQUESTing {");
                if (!m_Semaphore.Wait(1000))
                {
                    Console.WriteLine("TASK   "+TaskID+      "TIMEOUT!!!");
                    return; 
                }


                try
                {
                    Console.WriteLine("TASK   "+ TaskID +    "Working........... ........" + i);
                    watch.Restart();
                    Thread.Sleep(rnd.Next(200,500));
                }
                finally
                {
                    Console.WriteLine("TASK    "+TaskID+    "REQUESTing }");
                    m_Semaphore.Release();
                    i++;
                }        
            }
        }
        static void Main(string[] args)
        {
            #region  线程安全
            _Tasks = new Task[_TaskNum];
            m_Semaphore = new SemaphoreSlim(MAX_RESOURCE);
            for (int i = 0; i <_TaskNum ; i++)
            {
                _Tasks[i] = Task.Factory.StartNew((num) =>
                {
                    var taskid = (int)num;
                    Work1(taskid);
                }, i);
            }
            var finalTask = Task.Factory.ContinueWhenAll(_Tasks, (tasks) =>
            {
                Task.WaitAll(_Tasks);
                Console.WriteLine("==========================================================");
                Console.WriteLine("ALL Phase is Completed");
                Console.WriteLine("==========================================================");
            });



            try
            {
                finalTask.Wait();
            }
            catch (AggregateException aex)
            {
                Console.WriteLine("Task failed And Canceled" + aex.ToString());
            }
            finally
            {
                m_Semaphore.Dispose();
            }
            Console.ReadLine();
            #endregion
            var A = new List<int> { 1, 1, 2, 3, 4 };
            var B = new List<int> { 4, 5, 5, 6, 7 };

            //var result=A.Concat(B).ToList();
            //var result = A.Union(B).ToList();
            var result = A.Intersect(B).ToList();

            /*
             A表数据：  
                ID Content  
                1 内容哈哈哈哈  
  
                B表数据：  
                ID AID UserID  
                1 1 2012  
                2 1 2013  
                3 1 2014  
  
                我要的数据集是：  
                ID Content UserIDs  
                1 内容哈哈哈哈 2012,2013,2014  
                
            /构造类  
                public class A  
                {  
                  public int ID{get;set;}  
                  public string Content{get;set;}  
                }  
  
                public class B  
                {  
                  public int ID{get;set;}  
                  public int AID{get;set;}  
                  public int UserID{get;set;}  
                }  

                //初始化数据：  
                var listA=new List<A>(){new A{ ID=1, Content="aaaaaaaaaa"}};  
                var listB=new List<B>()  
                {  
                    new B{ ID=1, AID=1, UserID=2012},  
                    new B{ ID=2, AID=1, UserID=2013},  
                    new B{ ID=3, AID=1, UserID=2014},  
                };  

                //Group Join的Lamda表达式写法：  
    var QueryWithLamda=listA.GroupJoin(listB,  
                        a=>a.ID,  
                        b=>b.AID,  
                        (a,t)=>new   
                            {  
                                ID=a.ID,  
                                Content=a.Content,  
                                UserIDs=string.Join(",",t.Select(x=>x.UserID.ToString()).ToArray())  
                            });  


              //Group Join的标准表达式写法：  
    var QueryWithStandard=from a in listA  
              join b in listB  
              on a.ID equals b.AID into t  
              select new   
                {  
                    ID=a.ID,  
                    Content=a.Content,  
                    UserIDs=string.Join(",",t.Select(x=>x.UserID.ToString()).ToArray())  
                };  
             */
            //初始化数据：  
            var listA = new List<A>()
            {
                new A { ID = 1, Content = "aaaaaaaaaa" },
                new A { ID=2,Content="bbbbbbbbb"},
                new A { ID=3,Content="ccccccccccc"}
            };

            var listB = new List<B>()
                {
                    new B{ ID=1, AID=1, UserID=2012},
                    new B{ ID=2, AID=1, UserID=2013},
                    new B{ ID=3, AID=1, UserID=2014},
                    new B { ID=4,AID=2,UserID=2015 }
                };

            /*var modelQueryable = listA.GroupJoin(listB, x => x.ID, y => y.AID, (x, y) => new
            {
                ID = x.ID,
                Content = x.Content,
                UserIDs = string.Join(",", y.Select(t => t.UserID))
            });*/



            var modelQueryable = from a in listA
                                 join b in listB
                     on a.ID equals b.AID into t
                                 select new
                                 {
                                     ID = a.ID,
                                     Content = a.Content,
                                     UserIDs=string.Join(",",t.Select(x=>x.UserID))
                                 };

            var modelQuery = listA.GroupJoin(listB, 
                x => x.ID, 
                y => y.AID, 
                (x, y) => new
                {
                    ID =x.ID,
                    Content =x.Content,
                    UserIDs =string.Join(",",y.Select(t=>t.UserID))
                });

            var model = modelQueryable.Select(t => new C
            {
                 id=t.ID,
                 content=t.Content,
                 aid=t.UserIDs
            });


            //Group Join的Lamda表达式写法：  
            //var QueryWithLamda = listA.GroupJoin(listB,
            //                    a => a.ID,
            //                    b => b.AID,
            //                    (a, t) => new
            //                    {
            //                        ID = a.ID,
            //                        Content = a.Content,
            //                        UserIDs = string.Join(",", t.Select(x => x.UserID.ToString()))
            //                    }).AsQueryable();

            //var model = listA.Join(listB, a => a.ID, b => b.AID, (a, t) => new { ID=a.ID,Content=a.Content,UserIDs=string.Join(",",t.) });
            //foreach (var item in result)
            //{
            //    Console.WriteLine(item);
            //}


          
        }

        public class A
        {
            public int ID { get; set; }
            public string Content { get; set; }
        }

        public class B
        {
            public int ID { get; set; }
            public int AID { get; set; }
            public int UserID { get; set; }
        }

        public class C
        {
            public int id { get; set; }
            public string content { get; set; }
            public string aid { get; set; }

        }
    }
}
