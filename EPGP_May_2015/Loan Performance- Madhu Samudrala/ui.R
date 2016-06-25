shinyServer(
  pageWithSidebar(
    headerPanel("Predict Bad Loan Applicant"),
    
    sidebarPanel(
      textInput("Min_FICO_Score","Min FICO Score",500),
      textInput("Max_FICO_Score","Max FICO Score",600),
      textInput("Revolving_Line_Utilization","Revolving Line Utilization",20),
      textInput("Credit_Inquiries_Past_6m","Credit Inquiries Past 6m",1),
      selectInput("Home_Ownership","Home Ownership", choices=c("RENT", "OWN","MORTGAGE")),
      textInput("Annual_Income","Annual_Income",75000),
      textInput("Loan_Amount","Loan_Amount",6000)
    ),
    mainPanel(
      h2 ('Probability of default'),
      h3 (textOutput ('loandf')),
      tags$style ("#loandf{color: red;
                  font-size: 25px;
                  font-style: bold;
                  }"
        ),
        
      #h2 ('You Entered'),
        
        tags$div(class="modal-footer", 
                 "Note: This is a capstone course project by Madhu Samudrala. The dataset used for prediction is 'Lending Club Loan Dataset' and the 
                 prediction algorithm is Random Forest.")
        )
    
))