using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;
using System.Xml;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Data;
using System.Diagnostics;
using System.Data.SqlClient;

namespace ScopusQueries
{
    class Program
    {
        // http://www.developers.elsevier.com/action/devprojects
        // http://api.elsevier.com/documentation/SCOPUSSearchAPI.wadl
        // string.Format("http://www.eu-project-sites.com/api.php?mode=AU-ID&count={0}&apikey={1}&ID={2}&view=standard&start=0", countEntries, apiKey, authorID)
        // AU-ID(7006521320+OR+6506037515)

        //public static string apiKey = "95235931ebb1682f4bb241d5b85ba280";
        public static string apiKey = "a67c47ae6eb8460606c4df18cc79793b";
        //public static string apiKey = "b937709df3ceb29238fd77bfe3403efb";
        public static int countEntries = 25;

        static void Main(string[] args)
        {

            //using (StreamWriter w = File.AppendText("log.txt"))
            //{

            //    w.Write("\r\nLog Entry : ");
            //    w.WriteLine("  trtyrery :");

            //}

            //Console.WriteLine("-- START function populateDBForSCI( A ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_0-A.html");
            //Console.WriteLine("-- START function populateDBForSCI( B ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_B.html");
            //Console.WriteLine("-- START function populateDBForSCI( C ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_C.html");
            //Console.WriteLine("-- START function populateDBForSCI( D ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_D.html");
            //Console.WriteLine("-- START function populateDBForSCI( E ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_E.html");
            //Console.WriteLine("-- START function populateDBForSCI( F ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_F.html");
            //Console.WriteLine("-- START function populateDBForSCI( H ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_H.html");
            //Console.WriteLine("-- START function populateDBForSCI( I ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_I.html");
            //Console.WriteLine("-- START function populateDBForSCI( J ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_J.html");
            //Console.WriteLine("-- START function populateDBForSCI( K ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_K.html");
            //Console.WriteLine("-- START function populateDBForSCI( L ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_L.html");
            //Console.WriteLine("-- START function populateDBForSCI( M ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_M.html");
            //Console.WriteLine("-- START function populateDBForSCI( N ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_N.html");
            //Console.WriteLine("-- START function populateDBForSCI( O ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_O.html");
            //Console.WriteLine("-- START function populateDBForSCI( P ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_P.html");
            //Console.WriteLine("-- START function populateDBForSCI( Q ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_Q.html");
            //Console.WriteLine("-- START function populateDBForSCI( R ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_R.html");
            //Console.WriteLine("-- START function populateDBForSCI( S ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_S.html");
            //Console.WriteLine("-- START function populateDBForSCI( T ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_T.html");
            //Console.WriteLine("-- START function populateDBForSCI( U ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_U.html");
            //Console.WriteLine("-- START function populateDBForSCI( V ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_V.html");
            //Console.WriteLine("-- START function populateDBForSCI( W ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_W.html");
            //Console.WriteLine("-- START function populateDBForSCI( X ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_X.html");
            //Console.WriteLine("-- START function populateDBForSCI( Y ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_Y.html");
            //Console.WriteLine("-- START function populateDBForSCI( Z ) -- ");
            //populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_Z.html");
            //Console.WriteLine("-- START function populateDBForSCI( G ) -- ");
            // populateDBForSCI("http://www.citefactor.org/journal-impact-factor-list-2014_G.html"); // start from 3

            /**
             * 
             * Gemizei o pinakas documents
             * o pinakas document_authors
             * kai o pinakas citation
             * */
            //Console.WriteLine("-- START function populateDBForAuthors() -- ");
            populateDBForAuthors();

            /**
             * 
             * Gemizei o 
             * */
            //Console.WriteLine("-- START function investigateAuthors() -- ");
            //investigateAuthors();

            Console.WriteLine("-- START function evaluateHIndex() --");
            evaluateHIndex();
            evaluateHIndexCCP();

            evaluateHIndexCCPTimePeriod(2011,2016);

           // populateDBForAuthorswithImpactFactor();


            Console.WriteLine("-- END OF PROGRAM --");
            Console.ReadKey();
        }
        public static void evaluateHIndexCCP()
        {

            MySql.Data.MySqlClient.MySqlConnection conSQL;

            string myConnectionString = "Server=localhost;Port=3306;Database=citations;Uid=root;Pwd=cris$14#@;charset=greek;";

            // MySqlConnection conSQL = new MySqlConnection();
            conSQL = new MySql.Data.MySqlClient.MySqlConnection();

            MySqlCommand com = new MySqlCommand();
            com.CommandType = CommandType.Text;
            com.Connection = conSQL;

            
            try
            {

               

                conSQL.ConnectionString = myConnectionString;

                conSQL.Open();
            }
            catch(Exception ex)
            {
                //Console.Out(ex);
            }

            List<int> IDs = new List<int>();
            DataSet ds = new DataSet();
            MySqlDataAdapter da = new MySqlDataAdapter("select * from author where active=1 order by id", conSQL);

            //MySqlCommand cmd

            da.Fill(ds);
            if (ds.Tables[0].Rows.Count > 0)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    int ID = Convert.ToInt32(dr["id"].ToString());
                    IDs.Add(ID);
                }
            }
            foreach (int ID in IDs)
            {
                int nDocuments = 0, nCitations = 0, hIndex = 0;
                ds = new DataSet();
                da = new MySqlDataAdapter(string.Format("SELECT distinct(document.id) FROM author_document,author,document where author.id=author_document.author_id and document.id=author_document.document_id and author.id={0} and document.year<=2016 and document.doc_unique = 0", ID), conSQL);
                da.Fill(ds);
                if (ds.Tables[0].Rows.Count > 0)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        // plithos documents tou sygkekrimenoy author
                        nDocuments++;
                    }
                }
                ds = new DataSet();
                // self_cited = 0 ==> dld pernei mono tis eteroanafores
                da = new MySqlDataAdapter(string.Format("select document_id,count(*) as cc from citation where self_cited=0 and year<=2016 and document_id in (SELECT distinct(document.id) FROM author_document,author,document where author.id=author_document.author_id and document.id=author_document.document_id and author.id={0} and document.year<=2016 and document.doc_unique = 0 ) group by document_id order by cc desc", ID), conSQL);
                da.Fill(ds);
                Boolean tt = false;
                if (ds.Tables[0].Rows.Count > 0)
                {             
                  
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        int citations = Convert.ToInt32(dr[1].ToString());
                        nCitations += citations;                      
                    }

                    // na ksekinaei apo 0
                    int count = 0;
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        int citations = Convert.ToInt32(dr[1].ToString());                     

                        if (count >= citations) { hIndex = count; tt = true; break; }
                        count++;
                    }

                    if (!tt) hIndex = ds.Tables[0].Rows.Count;
                }
                /*
                 * by vasilis
                 * */
                com.CommandText = string.Format(string.Format("insert into h_index (author_id , h_index_value) values ('{0}',{1})", ID, hIndex));
                com.ExecuteNonQuery();

                com.CommandText = string.Format(string.Format("insert into documents_per_author (author_id , documents_number) values ('{0}',{1})", ID, nDocuments));
                com.ExecuteNonQuery();

                com.CommandText = string.Format(string.Format("insert into citations_per_author (author_id , citations_number) values ('{0}',{1})", ID, nCitations));
                com.ExecuteNonQuery();
                Console.WriteLine("{0}\t{1}\t{2}\t{3}", ID, nDocuments, nCitations, hIndex);
            }

            conSQL.Close();
        }

        public static void evaluateHIndexCCPTimePeriod(int start_year, int end_year)
        {

            MySql.Data.MySqlClient.MySqlConnection conSQL;

            string myConnectionString = "Server=localhost;Port=3306;Database=citations;Uid=root;Pwd=cris$14#@;charset=greek;";

            // MySqlConnection conSQL = new MySqlConnection();
            conSQL = new MySql.Data.MySqlClient.MySqlConnection();

            MySqlCommand com = new MySqlCommand();
            com.CommandType = CommandType.Text;
            com.Connection = conSQL;


            try
            {
                conSQL.ConnectionString = myConnectionString;
                conSQL.Open();
            }
            catch (Exception ex)
            {
                //Console.Out(ex);
            }

            List<int> IDs = new List<int>();
            DataSet ds = new DataSet();
            MySqlDataAdapter da = new MySqlDataAdapter("select * from author where active=1 order by id", conSQL);

            //MySqlCommand cmd

            da.Fill(ds);
            if (ds.Tables[0].Rows.Count > 0)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    int ID = Convert.ToInt32(dr["id"].ToString());
                    IDs.Add(ID);
                }
            }
            foreach (int ID in IDs)
            {
                int nDocuments = 0, nCitations = 0, hIndex = 0;
                ds = new DataSet();
                da = new MySqlDataAdapter(string.Format("SELECT distinct(document.id) FROM author_document,author,document where author.id=author_document.author_id and document.id=author_document.document_id and author.id={0} and document.year>={1} and document.year<={2} and document.doc_unique = 0", ID, start_year, end_year), conSQL);
                da.Fill(ds);
                if (ds.Tables[0].Rows.Count > 0)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        // plithos documents tou sygkekrimenoy author
                        nDocuments++;
                    }
                }
                ds = new DataSet();
                da = new MySqlDataAdapter(string.Format("select document_id,count(*) as cc from citation where self_cited=0 and year<=2016 and document_id in (SELECT distinct(document.id) FROM author_document,author,document where author.id=author_document.author_id and document.id=author_document.document_id and author.id={0} and document.year>={1} and document.year<={2} and document.doc_unique = 0) group by document_id order by cc desc", ID, start_year, end_year), conSQL);
                da.Fill(ds);
                Boolean tt = false;
                if (ds.Tables[0].Rows.Count > 0)
                {
                    // na ksekinaei apo 0
                    int count = 0;
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        int citations = Convert.ToInt32(dr[1].ToString());
                        nCitations += citations;
                        if (count >= citations) { hIndex = count; tt = true; break; }
                        count++;
                    }
                    if (!tt) hIndex = ds.Tables[0].Rows.Count;
                }
                /*
                 * by vasilis
                 * */
                com.CommandText = string.Format(string.Format("update h_index SET h_index_value_time_period={0} WHERE author_id = {1}", hIndex, ID));
                com.ExecuteNonQuery();

                com.CommandText = string.Format(string.Format("update documents_per_author SET documents_number_time_period={0} WHERE author_id = {1}", nDocuments, ID));
                com.ExecuteNonQuery();

                com.CommandText = string.Format(string.Format("update citations_per_author SET citations_number_time_period={0} WHERE author_id = {1}", nCitations, ID));
                com.ExecuteNonQuery();
                Console.WriteLine("{0}\t{1}\t{2}\t{3}", ID, nDocuments, nCitations, hIndex);
            }

            conSQL.Close();
        }

        public static void evaluateHIndex()
        {
            MySql.Data.MySqlClient.MySqlConnection conSQL;

            string myConnectionString = "Server=localhost;Port=3306;Database=citations;Uid=root;Pwd=cris$14#@;charset=greek;";


            // MySqlConnection conSQL = new MySqlConnection();
 conSQL = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {

               

                conSQL.ConnectionString = myConnectionString;

                conSQL.Open();
            }
            catch (Exception ex)
            {
                //Console.Out(ex);
            }



            List<int> IDs = new List<int>();
            DataSet ds = new DataSet();
            MySqlDataAdapter da = new MySqlDataAdapter("select * from author where active=1 order by id", conSQL);
            da.Fill(ds);
            if (ds.Tables[0].Rows.Count > 0)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    int ID = Convert.ToInt32(dr["id"].ToString());
                    IDs.Add(ID);
                }
            }
            foreach (int ID in IDs)
            {
                ds = new DataSet();
                da = new MySqlDataAdapter(string.Format("select document_id,count(*) as cc from citation where self_cited=0 and document_id in (SELECT distinct(document.id) FROM author_document,author,document where author.id=author_document.author_id and document.id=author_document.document_id and author.id={0}) group by document_id order by cc desc", ID), conSQL);
                da.Fill(ds);
                Boolean tt = false;
                if (ds.Tables[0].Rows.Count > 0)
                {
                    int count = 1;
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        int citations = Convert.ToInt32(dr[1].ToString());
                        if (count >= citations) { Console.WriteLine("{0}\t{1}", ID, count); tt = true; break; }
                        count++;
                    }
                    if (!tt) Console.WriteLine("{0}\t{1}", ID, ds.Tables[0].Rows.Count);
                }
            }

            conSQL.Close();
        }
        public static void populateDBForSCI(string pageURL)
        {
            int i;

            MySql.Data.MySqlClient.MySqlConnection conSQL;

            string myConnectionString = "Server=localhost;Port=3306;Database=citations;Uid=root;Pwd=cris$14#@;charset=greek;";


            // MySqlConnection conSQL = new MySqlConnection();
        conSQL = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {

               

                conSQL.ConnectionString = myConnectionString;

                conSQL.Open();
            }
            catch (Exception ex)
            {
                //Console.Out(ex);
            }

            MySqlCommand com = new MySqlCommand();
            com.CommandType = CommandType.Text;
            com.Connection = conSQL;

            Console.WriteLine("---- Http Request : {0} ",pageURL);

            HttpWebRequest req = (HttpWebRequest)WebRequest.Create(pageURL);
            req.ContentType = "application/x-www-form-urlencoded";
            req.Timeout = 60000;
            HttpWebResponse res = (HttpWebResponse)req.GetResponse();
            Stream resst = res.GetResponseStream();
            StreamReader sr = new StreamReader(resst);
            string reply = sr.ReadToEnd();
            resst.Flush();
            resst.Close();
            res.Close();

             Console.WriteLine("---- Split  <TD DIR=LTR ALIGN=RIGHT> ");


            string[] parts = reply.Split(new string[] { "<TD DIR=LTR ALIGN=RIGHT>" }, StringSplitOptions.None);

             Console.WriteLine(string.Format("---- Array Size: {0} ", parts.Length));

            for (i = 2; i < parts.Length; i++)
           //for (i = 3; i < parts.Length; i++)
            {
                string[] partsMore = parts[i].Split(new string[] { "<TD" }, StringSplitOptions.None);
                string issn = partsMore[2].Replace(" DIR=LTR ALIGN=LEFT>", "").Substring(0, 9).Replace("-", "");
                string IFString = partsMore[3].Replace(" DIR=LTR ALIGN=LEFT>", "").Replace("</TD>\n", "");

                 Console.WriteLine(string.Format("---- A/A : {0}, ISSN : {1} ", i, issn));
                try
                {
                    double IF = Convert.ToDouble(IFString);
                    DataSet ds = new DataSet();
                    MySqlDataAdapter da = new MySqlDataAdapter(string.Format("select * from issn where name='{0}'", issn), conSQL);
                    da.Fill(ds);
                    if (ds.Tables[0].Rows.Count == 0)
                    {
                        com.CommandText = string.Format(string.Format("insert into issn (name,impact_factor) values ('{0}',{1})", issn, IF));
                        com.ExecuteNonQuery();
                         Console.WriteLine(string.Format("---- {0} inserted in DB with Impact Factor {1}", issn, IF));
                    }
                    else
                    {
                         Console.WriteLine("---- {0} already exist in DB ",  issn);
                    }
                }
                catch {
                     Console.WriteLine("Exception");

                                       

                }
            }

            conSQL.Close();
        }
        public static void investigateAuthors()
        {
            HashSet<string> types = new HashSet<string>();

            MySql.Data.MySqlClient.MySqlConnection conSQL;

            string myConnectionString = "Server=localhost;Port=3306;Database=citations;Uid=root;Pwd=cris$14#@;charset=greek;";


            // MySqlConnection conSQL = new MySqlConnection();
 conSQL = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {

               

                conSQL.ConnectionString = myConnectionString;

                conSQL.Open();
            }
            catch (Exception ex)
            {
                //Console.Out(ex);
            }

            MySqlCommand com = new MySqlCommand();
            com.CommandType = CommandType.Text;
            com.Connection = conSQL;

            DataSet ds = new DataSet(); 
            // local history
            MySqlDataAdapter da = new MySqlDataAdapter("select * from author where active=1 order by id", conSQL);
            da.Fill(ds);
            if (ds.Tables[0].Rows.Count > 0)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    int ID = Convert.ToInt32(dr["id"].ToString());
                    string name = dr["name"].ToString();
                    string authorID = dr["scopus_id"].ToString();
                     Console.WriteLine(name);
                    List<publication> publications = queriesByAuthor(conSQL, authorID);
                    foreach (publication pub in publications)
                    {
                        string ss = string.Format("{0}*{1}", pub.aggregationType, pub.subtypeDescription);
                        if (!types.Contains(ss)) types.Add(ss);
                    }
                }
            }
            foreach (string ss in types)
            {
                string[] parts = ss.Split('*');
                Debug.WriteLine("{0}\t{1}", parts[0], parts[1]);
            }

            conSQL.Close();
        }
        public static void populateDBForAuthorswithImpactFactor()
        {
            MySql.Data.MySqlClient.MySqlConnection conSQL;

            string myConnectionString = "Server=localhost;Port=3306;Database=citations;Uid=root;Pwd=cris$14#@;charset=greek;";


            // MySqlConnection conSQL = new MySqlConnection();
conSQL = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {

                

                conSQL.ConnectionString = myConnectionString;

                conSQL.Open();
            }
            catch (Exception ex)
            {
                //Console.Out(ex);
            }

            MySqlCommand com = new MySqlCommand();
            com.CommandType = CommandType.Text;
            com.Connection = conSQL;

            DataSet ds = new DataSet();
            MySqlDataAdapter da = new MySqlDataAdapter("select * from author where active=1 order by id", conSQL);
            da.Fill(ds);
            if (ds.Tables[0].Rows.Count > 0)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    int ID = Convert.ToInt32(dr["id"].ToString());
                    string name = dr["name"].ToString();
                    string authorID = dr["scopus_id"].ToString();
                     Console.WriteLine(name);
                    List<publication> publications = queriesByAuthor(conSQL, authorID);
                    foreach (publication pub in publications)
                    {
                        if ((pub.impactFactor >- 0.0) && (pub.coverDate.Year == 2016))
                        {
                            DataSet dsID = new DataSet();
                            MySqlDataAdapter daID = new MySqlDataAdapter(string.Format("select * from document_impact_factor where scopus_id='{0}'", pub.publicationID), conSQL);
                            daID.Fill(dsID);
                            if (dsID.Tables[0].Rows.Count > 0) pub.documentID = Convert.ToInt32(dsID.Tables[0].Rows[0][0].ToString());
                            else
                            {
                                dsID = new DataSet();
                                daID = new MySqlDataAdapter("select * from document_impact_factor where id=0", conSQL);
                                MySqlCommandBuilder cbID = new MySqlCommandBuilder(daID);
                                daID.Fill(dsID);
                                DataRow drID = dsID.Tables[0].NewRow();
                                drID["details"] = pub.details;
                                drID["scopus_id"] = pub.publicationID;
                                drID["impact_factor"] = pub.impactFactor;
                                drID["year"] = pub.coverDate.Year;
                                drID["eid"] = pub.eid;
                                drID["doi"] = pub.doi;
                                drID["citedby_count"] = pub.citedby_count;
                                drID["document_type"] = pub.aggregationType;
                                drID["document_subtype"] = pub.subtypeDescription;
                                drID["issn"] = pub.issn;
                                dsID.Tables[0].Rows.Add(drID);
                                daID.InsertCommand = cbID.GetInsertCommand();
                                daID.UpdateCommand = cbID.GetUpdateCommand();
                                daID.DeleteCommand = cbID.GetDeleteCommand();
                                daID.Update(dsID);
                                dsID.AcceptChanges();

                                dsID = new DataSet();
                                daID = new MySqlDataAdapter(string.Format("select * from document_impact_factor where scopus_id='{0}'", pub.publicationID), conSQL);
                                daID.Fill(dsID);
                                pub.documentID = Convert.ToInt32(dsID.Tables[0].Rows[0][0].ToString());
                            }
                        }
                    }
                     Console.WriteLine("*** END OF PUBLICATIONS ***");
                }
            }

            conSQL.Close();
        }
        public static void populateDBForAuthors()
        {
            int i;

            /**
             * 
             * syndesi sti basi
             * */
            MySql.Data.MySqlClient.MySqlConnection conSQL;
            string myConnectionString = "Server=localhost;Port=3306;Database=citations;Uid=root;Pwd=cris$14#@;charset=greek;";
            conSQL = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {
                conSQL.ConnectionString = myConnectionString;
                conSQL.Open();
            }
            catch (Exception ex)
            {
                //Console.Out(ex);
            }

            MySqlCommand com = new MySqlCommand();
            com.CommandType = CommandType.Text;
            com.Connection = conSQL;

            DataSet ds = new DataSet();

            Console.WriteLine("SELECT AUTHORS: select * from author where active=1 order by id");

            /**
              * 
              * epilogi ton active authors
              * */
            MySqlDataAdapter da = new MySqlDataAdapter("select * from author where active=1 order by id", conSQL);
            da.Fill(ds);
            /**
             * 
             * elegxos an yparxoyn active authors
             * */
            if (ds.Tables[0].Rows.Count > 0)
            {
                /**
             * 
             * gia kathe author dimiourgoyme tis metavlites
             * ID (db auto-increment id)
             * name (author name)
             * authorID (author id in SCOPUS)
             * */
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    int ID = Convert.ToInt32(dr["id"].ToString());
                    string name = dr["name"].ToString();
                    string authorID = dr["scopus_id"].ToString();


                    Console.WriteLine("---- USER--> name: {0}, auhtorID: {1} ",name, authorID);


                    List<publication> publications = queriesByAuthor(conSQL, authorID);

                    Console.WriteLine("---- Start Foreach Publications for user {0}", name);

                    foreach (publication pub in publications)
                    {
                        DataSet dsID = new DataSet();
                        MySqlDataAdapter daID = new MySqlDataAdapter(string.Format("select * from document where scopus_id='{0}'", pub.publicationID), conSQL);
                        daID.Fill(dsID);
                        if (dsID.Tables[0].Rows.Count > 0) pub.documentID = Convert.ToInt32(dsID.Tables[0].Rows[0][0].ToString());
                        else
                        {
                            dsID = new DataSet();
                            daID = new MySqlDataAdapter("select * from document where id=0", conSQL);
                            MySqlCommandBuilder cbID = new MySqlCommandBuilder(daID);
                            daID.Fill(dsID);
                            DataRow drID = dsID.Tables[0].NewRow();
                            drID["details"] = pub.details;
                            drID["title"] = pub.title;
                            drID["publication_name"] = pub.publicationName;
                            drID["isbn"] = pub.isbn;
                            drID["volume"] = pub.volume;
                            drID["issue"] = pub.issueIdentifier;
                            drID["page_range"] = pub.pageRange;
                            drID["cover_date"] = pub.coverDate1;
                            drID["cover_displayed_date"] = pub.coverDate2;
                            drID["description"] = pub.description;
                            drID["scopus_id"] = pub.publicationID;
                            drID["year"] = pub.coverDate.Year;
                            drID["eid"] = pub.eid;
                            drID["doi"] = pub.doi;
                            drID["citedby_count"] = pub.citedby_count;
                            drID["document_type"] = pub.aggregationType;
                            drID["document_subtype"] = pub.subtypeDescription;
                            drID["issn"] = pub.issn;
                            //drID["sci"] = pub.hasImpactFactor ? 1 : 0;
                            dsID.Tables[0].Rows.Add(drID);
                            daID.InsertCommand = cbID.GetInsertCommand();
                            daID.UpdateCommand = cbID.GetUpdateCommand();
                            daID.DeleteCommand = cbID.GetDeleteCommand();
                            daID.Update(dsID);
                            dsID.AcceptChanges();

                            dsID = new DataSet();
                            daID = new MySqlDataAdapter(string.Format("select * from document where scopus_id='{0}'", pub.publicationID), conSQL);
                            daID.Fill(dsID);
                            pub.documentID = Convert.ToInt32(dsID.Tables[0].Rows[0][0].ToString());
                        }
                        dsID = new DataSet();
                        daID = new MySqlDataAdapter(string.Format("select * from author_document where author_id={0} and document_id={1}", ID, pub.documentID), conSQL);
                        daID.Fill(dsID);
                        if (dsID.Tables[0].Rows.Count == 0)
                        {
                            com.CommandText = string.Format(string.Format("insert into author_document (author_id,document_id) values ({0},{1})", ID, pub.documentID));
                            com.ExecuteNonQuery();

                            Console.WriteLine("insert into author_document values: {0},{1}", ID, pub.documentID);
                        }

                      

                    }
                    Console.WriteLine("*** END OF PUBLICATIONS ***");
                    foreach (publication pub in publications)
                    {
                        pub.citations = queriesByCitation(conSQL, pub.publicationID);
                        if (pub.citations == null)
                        {
                            continue;
                        }
                        foreach (publication citation in pub.citations)
                        {
                            for (i = 0; i < citation.brotherIDs.Count; i++)
                            {
                                if (authorID.Contains(citation.brotherIDs[i])) break;
                            }
                            if (i < citation.brotherIDs.Count) citation.selfCited = true;
                            DataSet dsID = new DataSet();
                            MySqlDataAdapter daID = new MySqlDataAdapter(string.Format("select * from citation where document_id={0} and cited_by_scopus_id='{1}'", pub.documentID, citation.publicationID), conSQL);
                            daID.Fill(dsID);
                            if (dsID.Tables[0].Rows.Count == 0)
                            {

                                //com.CommandText = string.Format(string.Format("insert into citation (document_id,cited_by_scopus_id,year,self_cited,sci) values ({0},'{1}',{2},{3},{4})", pub.documentID, citation.publicationID, citation.coverDate.Year, citation.selfCited ? 1 : 0, citation.hasImpactFactor ? 1 : 0));
                                com.CommandText = string.Format(string.Format("insert into citation (document_id,cited_by_scopus_id,year,self_cited,citation_type,citation_subtype) values ({0},'{1}',{2},{3},'{4}','{5}')", pub.documentID, citation.publicationID, citation.coverDate.Year, citation.selfCited ? 1 : 0, citation.aggregationType, citation.subtypeDescription));
                                com.ExecuteNonQuery();

                                

                                Console.WriteLine("---- INSERT Citation  ({0},'{1}',{2},{3},'{4}','{5}')", pub.documentID, citation.publicationID, citation.coverDate.Year, citation.selfCited, citation.aggregationType, citation.subtypeDescription);
                            }
                        }

                        //vasilis
                        com.CommandText = string.Format(string.Format("update document SET citedby_count_2 = {0} WHERE id = {1}", pub.citations.Count, pub.documentID));
                        com.ExecuteNonQuery();
                    }
                     Console.WriteLine("*** END OF CITATIONS ***");

                    //gveranis
                    foreach (publication pub in publications)
                    {                        
                        DataSet doID = new DataSet();
                        MySqlDataAdapter doiID = new MySqlDataAdapter(string.Format("select * from document where doi='{0}' AND doc_unique = 0 AND doi!='' ", pub.doi), conSQL);
                        doiID.Fill(doID);
                        if (doID.Tables[0].Rows.Count > 0)
                        {
                            //TODO if a document has a doi that is same as another one , then choose that with max citation
                            int doc_chain_id=0;
                            int countdoi = 0;
                            foreach (DataRow di in doID.Tables[0].Rows)
                            {                                
                                doc_chain_id = Convert.ToInt32(di["id"].ToString());
                                if (pub.documentID != doc_chain_id)
                                {
                                    countdoi++;
                                }
                            }

                            if (countdoi > 0)
                            {
                                //FOR THE MOMENT 
                                com.CommandText = string.Format(string.Format("update document SET doc_unique=1 , doc_chain={1}  WHERE id = {0}", pub.documentID, doc_chain_id));
                                com.ExecuteNonQuery();
                            }


                        }
                    }
                }
            }

            conSQL.Close();
        }
        public static List<publication> queriesByAuthor(MySql.Data.MySqlClient.MySqlConnection conSQL, string authorID)
        {           
            int ip;

           

            List<publication> publications = new List<publication>();

            HttpWebRequest req = (HttpWebRequest)WebRequest.Create(string.Format("http://api.elsevier.com/content/search/index:SCOPUS?query=AU-ID({0})&sort=coverDate&count={1}&start=0&apiKey={2}&view=complete&xml-decode=true&httpAccept=application/xml", authorID, countEntries, apiKey));
            req.Method = "GET";
            req.ContentType = "application/x-www-form-urlencoded";
            req.Timeout = 480000;
            HttpWebResponse res = (HttpWebResponse)req.GetResponse();
            Stream resst = res.GetResponseStream();
            StreamReader sr = new StreamReader(resst);
            string reply = sr.ReadToEnd();
            resst.Flush();
            resst.Close();
            res.Close();

            if (reply != "")
            {
                int totalEntries = 0;
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(reply);
                XmlNodeList entries = doc.GetElementsByTagName("opensearch:totalResults");
                foreach (XmlNode entry in entries)
                {
                    XmlElement element = (XmlElement)entry;
                    totalEntries = Convert.ToInt32(element.InnerText);
                }

                if (totalEntries > 0)
                {
                    for (ip = 0; ip < totalEntries; ip += countEntries)
                    {
                        if (ip > 0)
                        {
                            req = (HttpWebRequest)WebRequest.Create(string.Format("http://api.elsevier.com/content/search/index:SCOPUS?query=AU-ID({0})&sort=coverDate&count={1}&start={2}&view=complete&apiKey={3}&view=standard&xml-decode=true&httpAccept=application/xml", authorID, countEntries, ip, apiKey));
                            req.Method = "GET";
                            req.ContentType = "application/x-www-form-urlencoded";
                            req.Timeout = 980000;
                            res = (HttpWebResponse)req.GetResponse();
                            resst = res.GetResponseStream();
                            sr = new StreamReader(resst);
                            reply = sr.ReadToEnd();
                            resst.Flush();
                            resst.Close();
                            res.Close();
                        }

                        if (reply != "")
                        {
                            doc = new XmlDocument();
                            doc.LoadXml(reply);

                            entries = doc.GetElementsByTagName("entry");
                            foreach (XmlNode entry in entries)
                            {
                                XmlElement element = (XmlElement)entry;
                                publication pub = new publication();
                                XmlNodeList childs = element.ChildNodes;
                                foreach (XmlNode child in childs)
                                {


                                    XmlElement ele = (XmlElement)child;
                                    if (ele.Name == "dc:title") pub.title = ele.InnerText;
                                    else if (ele.Name == "prism:publicationName") pub.publicationName = ele.InnerText;
                                    else if (ele.Name == "prism:volume") pub.volume = ele.InnerText;
                                    else if (ele.Name == "prism:issueIdentifier") pub.issueIdentifier = ele.InnerText;
                                    else if (ele.Name == "prism:pageRange") pub.pageRange = ele.InnerText;
                                    else if (ele.Name == "prism:coverDate")
                                    {

                                        pub.coverDate1 = ele.InnerText;

                                        string[] dateParts = ele.InnerText.Split(new string[] { "-" }, StringSplitOptions.None);

                                        if (dateParts[1] == "00")
                                        {
                                            string newDate = dateParts[0] + "-01-01";
                                            pub.coverDate = DateTime.Parse(newDate);
                                        }
                                        else
                                        {
                                            pub.coverDate = DateTime.Parse(ele.InnerText);
                                        }

                                    }

                                    else if (ele.Name == "prism:coverDisplayDate") pub.coverDate2 = ele.InnerText;
                                    else if (ele.Name == "prism:aggregationType") pub.aggregationType = ele.InnerText;
                                    else if (ele.Name == "subtypeDescription") pub.subtypeDescription = ele.InnerText;
                                    else if (ele.Name == "prism:issn") pub.issn = ele.InnerText;
                                    else if (ele.Name == "prism:isbn") pub.isbn = ele.InnerText;
                                    else if (ele.Name == "eid") pub.eid = ele.InnerText;
                                    else if (ele.Name == "prism:doi") pub.doi = ele.InnerText;
                                    else if (ele.Name == "citedby-count") pub.citedby_count = ele.InnerText;
                                    else if (ele.Name == "prism:aggregationType") pub.document_type = ele.InnerText;
                                    else if (ele.Name == "subtypeDescription") pub.document_subtype = ele.InnerText;
                                    else if (ele.Name == "dc:description") pub.description = ele.InnerText;
                                    else if (ele.Name == "dc:identifier") pub.publicationID = ele.InnerText.Replace("SCOPUS_ID:", "");

                                    /*      else if (ele.Name == "authkeywords")
                                                   {                                        
                                                      pub.keywords = ele.InnerText.ToString();
                                                      string[] authKW = pub.keywords.Split( '|' );
                                                          foreach (string kw in authKW)
                                                            {
                                                              kw.Trim(); 
                                                              Console.WriteLine(kw);
                                                            }                                                           
                                                     }  gourgiotopoulos */

                                    else if (ele.Name == "author")
                                    {
                                        XmlNodeList moreChilds = ele.ChildNodes;
                                        foreach (XmlNode moreChild in moreChilds)
                                        {
                                            XmlElement e = (XmlElement)moreChild;
                                            if (e.Name == "authid")
                                            {
                                                string brotherID = e.InnerText;
                                                if (!pub.brotherIDs.Contains(brotherID)) pub.brotherIDs.Add(brotherID);
                                            }
                                            else if (e.Name == "authname")
                                            {
                                                if (!pub.details.Contains(e.InnerText + ", ")) pub.details += e.InnerText + ", ";
                                            }
                                        }
                                    }                     }
                                pub.details += pub.title;
                                pub.details += ", " + pub.publicationName;
                                if (pub.volume != null) pub.details += ", " + pub.volume;
                                if (pub.issueIdentifier != null) pub.details += " (" + pub.issueIdentifier + ")";
                                if (pub.pageRange != null) pub.details += ", " + pub.pageRange;
                                pub.details += ", " + pub.coverDate.Year.ToString();
                                // if(pub.keywords != null) pub.details += ", " + pub.keywords; --> prosthetw keywords KAI sta details. Gourgiotopoulos

                                //DataSet ds = new DataSet();
                                //MySqlDataAdapter da = new MySqlDataAdapter(string.Format("select * from issn where name='{0}'", pub.issn), conSQL);
                                //da.Fill(ds);
                                //if (ds.Tables[0].Rows.Count > 0) pub.hasImpactFactor = true;

                                //DataSet ds = new DataSet();
                                //MySqlDataAdapter da = new MySqlDataAdapter(string.Format("select * from issn where name='{0}'", pub.issn), conSQL);
                                //da.Fill(ds);
                                //if (ds.Tables[0].Rows.Count > 0) pub.impactFactor = Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString());

                                // if (pub.aggregationType == "Journal") publications.Add(pub);
                                publications.Add(pub);
                            }
                        }
                    }
                }
            }

            return publications;
        }
        public static List<publication> queriesByCitation(MySql.Data.MySqlClient.MySqlConnection conSQL, string documentID)
        {
            try
            {

                int ip;

                int totalCitations = 0;
                int totalErrorCitations = 0;

                List<publication> publications = new List<publication>();

               // Console.WriteLine(string.Format("http://api.elsevier.com/content/search/index:SCOPUS?query=REF({0})&sort=coverDate&count={1}&start=0&apiKey={2}&view=complete&xml-decode=true&httpAccept=application/xml", documentID, countEntries, apiKey));
              //  Console.ReadKey();
                HttpWebRequest req = (HttpWebRequest)WebRequest.Create(string.Format("http://api.elsevier.com/content/search/index:SCOPUS?query=REF({0})&sort=coverDate&count={1}&start=0&apiKey={2}&view=complete&xml-decode=true&httpAccept=application/xml", documentID, countEntries, apiKey));
                req.Method = "GET";
                req.ContentType = "application/x-www-form-urlencoded";
                req.Timeout = 60000;

                HttpWebResponse res = (HttpWebResponse)req.GetResponse();


                Stream resst = res.GetResponseStream();
                StreamReader sr = new StreamReader(resst);
                string reply = sr.ReadToEnd();
                resst.Flush();
                resst.Close();
                res.Close();

                if (reply != "")
                {
                    int totalEntries = 0;
                    XmlDocument doc = new XmlDocument();
                    doc.LoadXml(reply);
                    XmlNodeList entries = doc.GetElementsByTagName("opensearch:totalResults");
                    foreach (XmlNode entry in entries)
                    {
                        XmlElement element = (XmlElement)entry;
                        totalEntries = Convert.ToInt32(element.InnerText);
                    }

                    if (totalEntries > 0)
                    {
                        for (ip = 0; ip < totalEntries; ip += countEntries)
                        {

                            totalCitations++;
                            Debug.WriteLine("{0}", totalCitations);

                            if (ip > 0)
                            {
                                req = (HttpWebRequest)WebRequest.Create(string.Format("http://api.elsevier.com/content/search/index:SCOPUS?query=REF({0})&sort=coverDate&count={1}&start={2}&view=complete&apiKey={3}&view=standard&xml-decode=true&httpAccept=application/xml", documentID, countEntries, ip, apiKey));
                                req.Method = "GET";
                                req.ContentType = "application/x-www-form-urlencoded";
                                req.Timeout = 90000;


                                try
                                {
                                    res = (HttpWebResponse)req.GetResponse();
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("API ERROR: Document ID: {0} {1}", documentID, ex);
                                    totalErrorCitations++;

                                    Debug.WriteLine("{0}", totalErrorCitations);

                                                                try
                                                                {
                                                                    res = (HttpWebResponse)req.GetResponse();
                                                                }
                                                                catch (Exception ex2)
                                                                {
                                                                    Console.WriteLine("\r\n2 API ERROR: Document ID: {0} {1}", documentID, ex2);
                                                                    totalErrorCitations++;

                                                                    Debug.WriteLine("{0}", totalErrorCitations);

                                                                    continue;
                                                                }
                                }

                                resst = res.GetResponseStream();
                                sr = new StreamReader(resst);
                                reply = sr.ReadToEnd();
                                resst.Flush();
                                resst.Close();
                                res.Close();
                            }

                            if (reply != "")
                            {
                                doc = new XmlDocument();
                                doc.LoadXml(reply);

                                entries = doc.GetElementsByTagName("entry");
                                foreach (XmlNode entry in entries)
                                {
                                    XmlElement element = (XmlElement)entry;
                                    publication pub = new publication();
                                    XmlNodeList childs = element.ChildNodes;
                                    foreach (XmlNode child in childs)
                                    {
                                        XmlElement ele = (XmlElement)child;
                                        if (ele.Name == "dc:title") pub.title = ele.InnerText;
                                        else if (ele.Name == "prism:publicationName") pub.publicationName = ele.InnerText;
                                        else if (ele.Name == "prism:volume") pub.volume = ele.InnerText;
                                        else if (ele.Name == "prism:issueIdentifier") pub.issueIdentifier = ele.InnerText;
                                        else if (ele.Name == "prism:pageRange") pub.pageRange = ele.InnerText;
                                        else if (ele.Name == "prism:coverDate") pub.coverDate = DateTime.Parse(ele.InnerText);
                                        else if (ele.Name == "prism:aggregationType") pub.aggregationType = ele.InnerText;
                                        else if (ele.Name == "subtypeDescription") pub.subtypeDescription = ele.InnerText;
                                        else if (ele.Name == "prism:issn") pub.issn = ele.InnerText;
                                        else if (ele.Name == "eid") pub.eid = ele.InnerText;
                                        else if (ele.Name == "prism:doi") pub.doi = ele.InnerText;
                                        else if (ele.Name == "citedby-count") pub.citedby_count = ele.InnerText;
                               
                                        else if (ele.Name == "dc:identifier") pub.publicationID = ele.InnerText.Replace("SCOPUS_ID:", "");
                                        else if (ele.Name == "author")
                                        {
                                            XmlNodeList moreChilds = ele.ChildNodes;
                                            foreach (XmlNode moreChild in moreChilds)
                                            {
                                                XmlElement e = (XmlElement)moreChild;
                                                if (e.Name == "authid")
                                                {
                                                    string brotherID = e.InnerText;
                                                    pub.brotherIDs.Add(brotherID);
                                                }
                                            }
                                        }
                                    }

                                    //DataSet ds = new DataSet();
                                    //MySqlDataAdapter da = new MySqlDataAdapter(string.Format("select * from issn where name='{0}'", pub.issn), conSQL);
                                    //da.Fill(ds);
                                    //if (ds.Tables[0].Rows.Count > 0) pub.hasImpactFactor = true;

                                    // only Journals
                                    //if (pub.aggregationType == "Journal") publications.Add(pub);
                                    publications.Add(pub);
                                }
                            }
                        }
                    }
                }

                return publications;
            }catch
            {
                return null;
            }
        }
    }

    public class publication
    {
        public int documentID;
        public string publicationID;
        public DateTime coverDate;
        //public Boolean selfCited, hasImpactFactor;
        public double impactFactor;
        public Boolean selfCited;
        public string title, publicationName, volume, issueIdentifier, pageRange, aggregationType, coverDate1, coverDate2, subtypeDescription, description, details, issn, isbn, eid, doi, citedby_count, document_type, document_subtype, doc_unique, doc_chain; //,keywords;
        public List<string> brotherIDs;
        public List<publication> citations; 
        public publication()
        {
            impactFactor = -1.0;
            documentID = 0;
            //selfCited = hasImpactFactor = false;
            selfCited = false;
            details = aggregationType = subtypeDescription = eid = doi = citedby_count = document_type = document_subtype = doc_unique = doc_chain = "";
            brotherIDs = new List<string>();
            citations = new List<publication>();
        }
    }
}
