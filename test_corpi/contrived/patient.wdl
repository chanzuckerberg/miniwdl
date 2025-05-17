version 1.1
# from https://github.com/chanzuckerberg/miniwdl/issues/635

import "person_struct.wdl"
  alias Person as Patient
  alias Income as PatientIncome

# This struct has the same name as a struct in 'person_struct.wdl',
# but they have identical definitions so an alias is not required.
struct Name {
  String first
  String last
}

# This struct also has the same name as a struct in 'structs.wdl',
# but their definitions are different, so it was necessary to
# import the struct under a different name.
struct Income {
  Float dollars
  Boolean annual
}

struct Person {
  Int age
  Name name
  Float? height
  Income income
}

task calculate_bill {
  input {
    Person doctor
    Patient patient
    PatientIncome average_income = PatientIncome {
      amount: 50000,
      currency: "USD",
      period: "annually"
    }
  }
  
  PatientIncome income = select_first([patient.income, average_income])
  String currency = select_first([income.currency, "USD"])
  Float hourly_income = if income.period == "hourly" then income.amount else income.amount / 2000
  Float hourly_income_usd = if currency == "USD" then hourly_income else hourly_income * 100

  command <<<
  printf "The patient makes $~{hourly_income_usd} per hour\n~{doctor.name}"
  >>>
  
  output {
    Float amount = hourly_income_usd * 5
  }
}

workflow import_structs {
  input {
    Person doctor = Person {
      age: 10,
      name: Name {
        first: "Joe",
        last: "Josephs"
      },
      income: Income {
        dollars: 140000,
        annual: true
      }
    }

    Patient patient = Patient {
      name: Name {
        first: "Bill",
        last: "Williamson"
      },
      age: 42,
      income: PatientIncome {
        amount: 350,
        currency: "Yen",
        period: "hourly"
      },
      assay_data: {
        "glucose": "hello.txt"
      }
    }
  }

  #call person_struct.greet_person {
  #  input: person = patient
  #}

  call calculate_bill {
    input: doctor = doctor, patient = patient
  }

  output {
    Float bill = calculate_bill.amount
  }
}
