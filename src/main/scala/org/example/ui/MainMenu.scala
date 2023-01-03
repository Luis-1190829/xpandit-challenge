package org.example.ui

import org.example.bigdata.Option1
import org.example.bigdata.Option2
import org.example.bigdata.Option3
import org.example.bigdata.Option4
import org.example.bigdata.Option5

import scala.io.StdIn

class MainMenu {

  private val OPTIONS_DESCRIPTIONS = Array(
    "1 - Create dataframe(df_1) from googleplaystore_user_reviews.csv with Average_Sentiment_Polarity",
    "2 - Read googleplaystore.csv as a Dataframe and obtain all apps with a rating greater\n\tor equal to 4.0 sorted in " +
      "descending order",
    "3 - Create dataframe(df_3) from googleplaystore.csv",
    "4 - Given the dataframes produced by option 1 and 3, create a dataframe with all its\n\tinformation plus its " +
      "'Average_Sentiment_Polarity' calculated in option 1",
    "5 - Use df_3 to create a new Dataframe (df_4) containing the number of applications,\n\tthe average rating and the " +
      "average sentiment polarity by genre",
    "0 - Exit")

  private val option1 = new Option1();
  private val option2 = new Option2();
  private val option3 = new Option3();
  private val option4 = new Option4();
  private val option5 = new Option5();

  private val MAIN_MENU_LABEL = "------------------------------ Main Menu ------------------------------"
  private val MAIN_MENU_SEPARATOR = "-----------------------------------------------------------------------\nSelect an option:"
  private val INVALID_SELECTED_OPTION = "Invalid option"

  def mainMenuLoop (): Unit = {
    var selectedOption = - 1
    while (selectedOption != 0) {
      printMainMenu()
      try{
        selectedOption = StdIn.readInt()
        selectedOption match {
          case 1 => println(s"Option $selectedOption selected")
            option1.option1Executer()
          case 2 => println(s"Option $selectedOption selected")
            option2.option2Executer()
          case 3 => println(s"Option $selectedOption selected")
            option3.option3Executer()
          case 4 => println(s"Option $selectedOption selected")
            option4.option4Executer(option1.df_1,option3.df_3)
          case 5 => println(s"Option $selectedOption selected")
            option5.option5Executer(option4.data)
          case 0 => println("Exiting ...")
          case default =>
            println(INVALID_SELECTED_OPTION)
        }
        println(s"Option $selectedOption processed")
        Thread.sleep(1000)
      } catch {
        case e : NumberFormatException => println(INVALID_SELECTED_OPTION)
        case e: Exception => println(e.getMessage)
      }
    }
  }

  private def printMainMenu(): Unit = {
    println(MAIN_MENU_LABEL)
    for(optionDescription  <- OPTIONS_DESCRIPTIONS){
      println(optionDescription)
    }
    print(MAIN_MENU_SEPARATOR)
  }
}
