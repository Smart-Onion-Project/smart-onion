import { CategoryInfo } from "../models/category-info/category-info.model";
import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http"
import { map } from 'rxjs/operators';
import { stringify } from 'querystring';

@Injectable({ providedIn: 'root' })
export class BackendService {
  private backendBaseUrl = "http://localhost:8080";
  private questionsAnswered = [];

  // constructor() {}
  constructor(private httpClient: HttpClient) {}

  //[
  //   {"category_id": "dfgdf-324532-43654546", "category_name": "blah blah"},
  //   {"category_id": "dfgdf-324532-43654546", "category_name": "blah blah"}
  //]
  getAllCategories() {
    var res : CategoryInfo[] = [];
    var rawBackendResponse : { category_id : string, category_name : string }[] = [];

    return this.httpClient.get(this.backendBaseUrl + "/get-all-categories")
    .pipe(map((responseData : {
      categories : {
        category_id : string,
        category_name : string
      }[]
    }) => {
      var categories : { category_id : string, category_name : string }[] = responseData.categories;
      categories.map(category => {
        res.push(new CategoryInfo(category.category_id, category.category_name));
      });

      return res;
    }))
  }

  getAllQueryIds() {

  }

  getNextRandomQuestion() {
    return this.httpClient.post(this.backendBaseUrl + "/get-random-question", {
      answered_questions_ids: this.questionsAnswered
    })
    .pipe(map((responseData : {
      question_id: string,
      metric_id: string,
      metric_name: string,
      anomaly_type_id: string,
      anomaly_type: string,
      description: string
    }) => {
      return responseData;
    }))
  }

  getGrade() {

  }

  reportQuery() {

  }

  addNewCategory() {

  }
}